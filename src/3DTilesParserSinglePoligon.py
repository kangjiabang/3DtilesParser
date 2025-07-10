import json
import uuid
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# py3dtiles
from py3dtiles.tileset import TileSet
import math

# PostGIS
import psycopg2
from psycopg2.extras import execute_batch

from shapely import wkt
from shapely.validation import explain_validity


def wgs84_to_ecef(lat_deg, lon_deg, alt_m):
    """将 WGS84 坐标转换为 ECEF 坐标"""
    a = 6378137.0          # 长半轴
    f = 1 / 298.257223563  # 扁率
    b = a * (1 - f)        # 短半轴
    e_sq = 2 * f - f ** 2   # 第一偏心率平方

    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)

    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    sin_lon = math.sin(lon)
    cos_lon = math.cos(lon)

    N = a / math.sqrt(1 - e_sq * sin_lat ** 2)
    x = (N + alt_m) * cos_lat * cos_lon
    y = (N + alt_m) * cos_lat * sin_lon
    z = ((b ** 2 / a ** 2) * N + alt_m) * sin_lat

    return x, y, z


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
def collect_tileset_bounds(tileset: TileSet, tileset_dir: str, base_transform=None) -> List[Dict]:
    """提取瓦片的包围盒底面为 POLYGON Z，并计算高度"""

    def _recursive_collect(tile):
        bounds_list = []

        bv = tile.bounding_volume
        if bv is not None:
            box = bv._box  # [cx, cy, cz, hx, hy, hz]

            # 使用改进的方法计算底面多边形
            points = box_to_polygonz(box)

            # 获取中心点和半长
            cx, cy, cz = box[0], box[1], box[2]
            dx, dy, dz = box[3], box[7], box[11]

            min_z = cz - dz
            max_z = cz + dz
            height = max_z - min_z

            tile_url = tile.content_uri if (hasattr(tile, 'content_uri') and tile.content_uri) else None
            # 只有当 tileUrl 存在时才进行后缀判断
            if tile_url is None:
                return []  # 跳过没有 URL 的瓦片

            # 使用 .suffix 属性判断是否为 .b3dm 文件
            if isinstance(tile_url, Path):
                if tile_url.suffix.lower() != ".b3dm":
                    return []
            else:
                # 如果是字符串，则统一处理
                if not str(tile_url).lower().endswith(".b3dm"):
                    return []

            # 生成 POLYGON Z 的 WKT 字符串
            coords_str = ", ".join(f"{x:.6f} {y:.6f} {z:.6f}" for x, y, z in points)
            polygon_ewkt = f"POLYGON Z (({coords_str}))"

            print(f"当前瓦片：{polygon_ewkt}")
            # 记录瓦片信息
            bounds_info = {
                "bounding_volume": {"to_ewkt": polygon_ewkt},
                "tile_url": tile_url,
                "refine": tile._refine,
                "properties": {"tileset_dir": tileset_dir},
                "parent_dir": str(Path(tileset_dir).parent.name),
                "height": height,  # 👈 新增高度字段
            }
            bounds_list.append(bounds_info)

        # 递归处理子瓦片
        for child in tile.children:
            bounds_list.extend(_recursive_collect(child))
        return bounds_list

    return _recursive_collect(tileset.root_tile)


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

        records.append((
            str(uuid.uuid4()),
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
    ROOT_TILESET_DIR = "../3dtiles"  # 根目录（包含根tileset.json）

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