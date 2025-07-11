import hashlib
import json
import uuid
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
# PostGIS
import psycopg2
from psycopg2.extras import execute_batch
# py3dtiles
from py3dtiles.tileset import TileSet


# =============================================
# 1️⃣ 加载单个 tileset 并记录其路径
# =============================================
def load_tileset_with_path(tileset_path: str) -> Tuple[TileSet, str]:
    """加载瓦片集并返回瓦片集对象和其所在目录路径"""
    path = Path(tileset_path)
    with path.open('r') as f:
        data = json.load(f)
    tileset = TileSet.from_dict(data)
    return tileset, str(path.parent.absolute())


# =============================================
# 2️⃣ 递归查找所有子目录中的 tileset.json
# =============================================
def find_all_tileset_files(root_dir: str) -> List[str]:
    """递归查找根目录及其所有子目录中的 tileset.json 文件"""
    root_path = Path(root_dir)
    tileset_files = list(root_path.rglob("tileset.json"))
    return [str(file.absolute()) for file in tileset_files]


def box_to_polygonz(box):
    """更精确的box到POLYGON Z转换，考虑旋转"""
    center = box[:3]
    x_axis = box[3:6]
    y_axis = box[6:9]
    z_axis = box[9:12]

    # 计算底面四个角（在局部坐标系中）
    corners_local = [
        (-1, -1, -1),  # 左下前
        (1, -1, -1),  # 右下前
        (1, 1, -1),  # 右上前
        (-1, 1, -1)  # 左上前
    ]

    # 转换到世界坐标系
    points = []
    for dx, dy, dz in corners_local:
        x = center[0] + dx * x_axis[0] + dy * y_axis[0] + dz * z_axis[0]
        y = center[1] + dx * x_axis[1] + dy * y_axis[1] + dz * z_axis[1]
        z = center[2] + dx * x_axis[2] + dy * y_axis[2] + dz * z_axis[2]
        points.append((x, y, z))

    # 闭合环
    points.append(points[0])

    return points


# =============================================
# 3️⃣ 提取单个瓦片集的所有 tile 包围盒（生成Box类型）
# =============================================
def collect_tileset_bounds(tileset: TileSet, tileset_dir: str) -> List[Dict]:
    """提取瓦片的包围盒底面为 POLYGON Z，并计算高度，考虑 transform 转换"""
    def _apply_transform(point, matrix):
        """将局部坐标点应用变换矩阵"""
        homogeneous_point = np.append(point, 1.0)
        transformed = matrix @ homogeneous_point
        return transformed[:3]  # 返回 xyz 部分

    def _matrix_from_column_major(array):
        """将列优先数组转为 4x4 矩阵"""
        return np.array(array, dtype=np.float64).reshape((4, 4), order='F')

    def _recursive_collect(tile, parent_transform=None):
        if parent_transform is None:
            parent_transform = np.eye(4)

        bounds_list = []

        # 确保 tile 是有效的 Tile 对象
        if not hasattr(tile, '__dict__'):
            return bounds_list

        # 更新当前 tile 的变换矩阵
        current_transform = parent_transform.copy()
        if hasattr(tile, 'transform') and tile.transform is not None:
            try:
                local_matrix = _matrix_from_column_major(tile.transform)
                current_transform = current_transform @ local_matrix
            except:
                pass  # 如果转换失败，继续使用父变换

        # 检查是否是叶子节点 (没有children或children为空)
        is_leaf = not hasattr(tile, 'children') or not getattr(tile, 'children', [])

        # 只处理叶子节点
        if is_leaf:
            # 检查是否有包围盒
            if hasattr(tile, 'bounding_volume') and tile.bounding_volume is not None:
                bv = tile.bounding_volume
                # 检查是否是box类型
                if hasattr(bv, '_box') and bv._box is not None:
                    try:
                        box = bv._box  # [cx, cy, cz, hx, hy, hz]

                        center_local = np.array(box[:3], dtype=np.float64)
                        x_axis = np.array(box[3:6], dtype=np.float64)
                        y_axis = np.array(box[6:9], dtype=np.float64)
                        z_axis = np.array(box[9:12], dtype=np.float64)

                        # 局部坐标系下的底面四个角点
                        corners_local = [
                            center_local - x_axis - y_axis - z_axis,
                            center_local + x_axis - y_axis - z_axis,
                            center_local + x_axis + y_axis - z_axis,
                            center_local - x_axis + y_axis - z_axis,
                            center_local - x_axis - y_axis + z_axis,
                            center_local + x_axis - y_axis + z_axis,
                            center_local + x_axis + y_axis + z_axis,
                            center_local - x_axis + y_axis + z_axis,
                        ]

                        # 应用变换矩阵到所有角点
                        corners_world = [_apply_transform(corner, current_transform) for corner in corners_local]

                        # 构造底面多边形（用于可视化/数据库）
                        bottom_corners_world = corners_world[:4]
                        bottom_corners_world.append(bottom_corners_world[0])  # 闭合环

                        coords_str = ", ".join(f"{x:.6f} {y:.6f} {z:.6f}" for x, y, z in bottom_corners_world)
                        polygon_ewkt = f"POLYGON Z (({coords_str}))"

                        # 获取高度信息（使用局部坐标包围盒的高度）
                        z_values = [corner[2] for corner in corners_local]
                        height = max(z_values) - min(z_values)

                        # 生成瓦片 URL（仅处理 .b3dm 文件）
                        tile_url = getattr(tile, 'content_uri', None)
                        if tile_url and str(tile_url).lower().endswith(".b3dm"):
                            bounds_info = {
                                "bounding_volume": {"to_ewkt": polygon_ewkt},
                                "tile_url": tile_url,
                                "refine": getattr(tile, '_refine', None),
                                "properties": {"tileset_dir": tileset_dir},
                                "height": height
                            }
                            bounds_list.append(bounds_info)
                    except Exception as e:
                        print(f"处理tile包围盒时出错: {e}")
                        pass  # 如果处理包围盒失败，继续下一个 tile
        # 递归处理子瓦片
        if hasattr(tile, 'children') and getattr(tile, 'children', None):
            for child in tile.children:
                try:
                    bounds_list.extend(_recursive_collect(child, current_transform))
                except:
                    continue

        return bounds_list

    return _recursive_collect(tileset.root_tile, np.eye(4))


# =============================================
# 5️⃣ 插入数据库和碰撞检测（基于BOX3D）
# =============================================
def insert_buildings_to_postgis(conn, building_data: List[Dict]):
    # 建表语句：使用BOX3D类型
    init_table_query = """
        CREATE TABLE IF NOT EXISTS dk_buildings (
        id UUID PRIMARY KEY,
        name TEXT,
        tile_url TEXT,
        bounding_volume GEOMETRY(POLYGONZ, 4978),  -- 改为 POLYGONZ 类型
        refine TEXT,
        properties JSONB,
        height NUMERIC  -- 👈 新增 height 字段
    );

    -- 创建空间索引
    CREATE INDEX IF NOT EXISTS idx_buildings_geom_3d 
    ON dk_buildings USING GIST (bounding_volume);
        """
    with conn.cursor() as cur:
        cur.execute(init_table_query)
    conn.commit()

    query = """
        INSERT INTO dk_buildings 
        (id, name, tile_url, bounding_volume, refine, properties, height)
        VALUES (%s, %s, %s, ST_GeomFromEWKT(%s), %s, %s, %s)
    """

    records = []
    for b in building_data:
        bv = b["bounding_volume"]
        # 生成带SRID的EWKT
        ewkt = f"SRID=4978;{bv['to_ewkt']}"
        print(f"当前瓦片：{ewkt}")
        tile_url = str(b.get("tile_url")) if b.get("tile_url") else None

        height = float(b.get("height")) if b.get("height") is not None else None  # 👈 强制转换为 float

        # 基于 tile_url 生成 MD5 ID
        if tile_url:
            # 创建 MD5 哈希对象
            md5_hash = hashlib.md5()
            # 更新哈希值（需要 encode 成 bytes）
            md5_hash.update(tile_url.encode('utf-8'))
            # 获取十六进制表示的哈希值
            tile_url_md5 = md5_hash.hexdigest()
        else:
            # 如果 tile_url 是 None，则回退到 UUID
            tile_url_md5 = str(uuid.uuid4())

        records.append((
            tile_url_md5,
            "Building",
            tile_url,
            ewkt,
            b.get("refine"),
            json.dumps(b.get("properties", {})),
            height,  # 👈 插入高度字段
        ))

    print(f"records: {records[:5]}...")  # 打印前5条记录以检查
    with conn.cursor() as cur:
        execute_batch(cur, query, records, page_size=100)
    conn.commit()
    print(f"✅ 成功插入 {len(records)} 条建筑物数据（包含高度）")


def check_collision_in_db(conn, point_x: float, point_y: float, point_z: float) -> List[Tuple]:
    """
    根据点的 (x, y, z) 判断其是否“碰撞”到某栋建筑物。
    使用两步判断：
        1. 点是否在建筑物的二维投影范围内；
        2. 点的 Z 是否在地面高程和建筑顶部之间；
    """
    query = """
        SELECT 
            id, 
            tile_url, 
            properties->>'tileset_dir' AS source_dir,
            height,
            ST_Z(ST_PointN(ST_ExteriorRing(bounding_volume), 1)) AS ground_z
        FROM 
            dk_buildings
        WHERE 
            -- 第一步：二维投影是否包含该点
            ST_Intersects(
                ST_Force2D(bounding_volume),
                ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4978)
            )
            AND
            -- 第二步：点的 Z 是否在地面与建筑高度之间
            %(z)s >= ST_Z(ST_PointN(ST_ExteriorRing(bounding_volume), 1))
            AND
            %(z)s <= ST_Z(ST_PointN(ST_ExteriorRing(bounding_volume), 1)) + height;
    """

    # 构造参数字典
    params = {
        "x": point_x,
        "y": point_y,
        "z": point_z
    }

    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


# =============================================
# 主函数
# =============================================
def main():
    DB_CONN_STRING = "dbname=nyc user=postgres password=123456 host=localhost port=5432"

    with psycopg2.connect(DB_CONN_STRING) as conn:
        # 初始化瓦片集并插入数据库
        #init_tileset(conn)

        # 模拟无人机碰撞检测
        drone_x, drone_y, drone_z = 1729.91997, -525.244258,-144.115917  # 无人机三维坐标
        point_ewkt = f"SRID=4978;POINT Z({drone_x} {drone_y} {drone_z})"
        print(f"模拟无人机位置: {point_ewkt}")

        print("\n🚨 正在进行碰撞检测...")
        collisions = check_collision_in_db(conn,  drone_x, drone_y, drone_z)

        if collisions:
            print("⚠️ 警告！与以下瓦片发生碰撞：")
            for c in collisions:
                print(f" - ID: {c[0]}, 瓦片文件: {c[1]}, 来源目录: {c[2]}, 建筑物高度: {c[3]:.2f} 米")
        else:
            print("✅ 安全：当前路径无碰撞风险。")


def init_tileset(conn):
    """初始化瓦片集，解析所有瓦片并插入数据库"""
    ROOT_TILESET_DIR = "../../3dtiles"  # 根目录（包含根tileset.json）

    print(f"🔍 正在查找 {ROOT_TILESET_DIR} 下的所有 tileset.json...")
    all_tileset_files = find_all_tileset_files(ROOT_TILESET_DIR)
    print(f"📋 找到 {len(all_tileset_files)} 个 tileset.json 文件")

    all_building_bounds = []
    for tileset_file in all_tileset_files:
        print(f"\n📦 正在解析 {tileset_file}...")
        tileset, tileset_dir = load_tileset_with_path(tileset_file)
        tiles_bounds = collect_tileset_bounds(tileset, tileset_dir)
        all_building_bounds.extend(tiles_bounds)
        print(f"📌 从该瓦片集提取 {len(tiles_bounds)} 个瓦片（BOX3D）")

    print(f"\n💾 总计解析 {len(all_building_bounds)} 个瓦片，准备插入数据库...")
    insert_buildings_to_postgis(conn, all_building_bounds)

    return all_building_bounds


if __name__ == "__main__":
    main()