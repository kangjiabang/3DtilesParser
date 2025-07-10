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
    # 返回瓦片集对象和其所在目录的绝对路径（用于关联子目录）
    return tileset, str(path.parent.absolute())


# =============================================
# 2️⃣ 递归查找所有子目录中的 tileset.json
# =============================================
def find_all_tileset_files(root_dir: str) -> List[str]:
    """递归查找根目录及其所有子目录中的 tileset.json 文件"""
    root_path = Path(root_dir)
    # 递归查找所有名为 tileset.json 的文件
    tileset_files = list(root_path.rglob("tileset.json"))
    # 转换为绝对路径字符串
    return [str(file.absolute()) for file in tileset_files]


# =============================================
# 3️⃣ 提取单个瓦片集的所有 tile 包围盒（支持标记父目录）
# =============================================
def collect_tileset_bounds(tileset: TileSet, tileset_dir: str, base_transform=None) -> List[Dict]:
    """
    提取单个瓦片集的所有瓦片包围盒
    :param tileset: 瓦片集对象
    :param tileset_dir: 瓦片集所在目录（用于记录来源）
    :param base_transform: 父瓦片的变换矩阵
    :return: 瓦片包围盒信息列表
    """
    def _recursive_collect(tile, current_transform):
        bounds_list = []
        # 确定当前瓦片的变换矩阵（优先使用自身transform，否则继承父transform）
        if hasattr(tile, 'transform'):
            tile_transform = tile.transform
        else:
            tile_transform = current_transform

        bv = tile.bounding_volume
        if bv is not None:
            box = bv._box  # [cx, cy, cz, hx, ..., hy, ..., hz]
            cx, cy, cz = box[0], box[1], box[2]
            hx = box[3]   # x方向半长
            hy = box[7]   # y方向半长
            hz = box[11]  # z方向半长

            # 构造8个顶点（局部坐标）
            points = [
                (cx - hx, cy - hy, cz - hz),
                (cx + hx, cy - hy, cz - hz),
                (cx + hx, cy + hy, cz - hz),
                (cx - hx, cy + hy, cz - hz),
                (cx - hx, cy - hy, cz + hz),
                (cx + hx, cy - hy, cz + hz),
                (cx + hx, cy + hy, cz + hz),
                (cx - hx, cy + hy, cz + hz),
            ]

            # 应用变换矩阵（转换为全局坐标）
            if tile_transform is not None:
                points = apply_transform(points, tile_transform)

            # 生成WKT
            polyhedron_wkt = create_polyhedron_wkt(points)

            # 记录瓦片信息（包含来源目录）
            bounds_info = {
                "bounding_volume": {"to_ewkt": lambda: polyhedron_wkt},
                "tile_url": tile.content_uri if (hasattr(tile, 'content_uri') and tile.content_uri) else None,
                "refine": tile._refine,
                "properties": {"tileset_dir": tileset_dir},  # 记录瓦片集来源目录
                "parent_dir": str(Path(tileset_dir).parent.name)  # 记录父目录（用于层级关联）
            }
            bounds_list.append(bounds_info)

        # 递归处理子瓦片
        for child in tile.children:
            bounds_list.extend(_recursive_collect(child, tile_transform))
        return bounds_list

    # 从根瓦片开始递归收集
    return _recursive_collect(tileset.root_tile, base_transform)


# =============================================
# 4️⃣ 坐标变换和WKT生成（复用原有逻辑，优化兼容性）
# =============================================
def apply_transform(points, transform_matrix):
    """应用4x4变换矩阵到三维点"""
    if len(transform_matrix) < 16:
        # 补全为4x4单位矩阵（仅保留平移）
        identity = [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]
        if len(transform_matrix) >= 3:
            identity[12] = transform_matrix[0]  # tx
            identity[13] = transform_matrix[1]  # ty
            identity[14] = transform_matrix[2]  # tz
        transform_matrix = identity

    def multiply(matrix, x, y, z, w=1):
        x_new = matrix[0]*x + matrix[4]*y + matrix[8]*z + matrix[12]*w
        y_new = matrix[1]*x + matrix[5]*y + matrix[9]*z + matrix[13]*w
        z_new = matrix[2]*x + matrix[6]*y + matrix[10]*z + matrix[14]*w
        w_new = matrix[3]*x + matrix[7]*y + matrix[11]*z + matrix[15]*w
        return (x_new / w_new, y_new / w_new, z_new / w_new)

    return [multiply(transform_matrix, x, y, z) for x, y, z in points]


def clean_points(raw_points):
    """清洗点坐标（处理可能的数组格式）"""
    cleaned = []
    for p in raw_points:
        # 处理元组内的数值（兼容普通数值或数组）
        x = p[0] if not hasattr(p[0], '__iter__') else p[0][0]
        y = p[1] if not hasattr(p[1], '__iter__') else p[1][0]
        z = p[2] if not hasattr(p[2], '__iter__') else p[2][0]
        cleaned.append((x, y, z))
    return cleaned


def create_polyhedron_wkt(points):
    """生成符合PostGIS规范的POLYHEDRALSURFACE Z WKT"""
    points = clean_points(points)
    faces = [
        [0, 1, 2, 3],  # 底面
        [4, 5, 6, 7],  # 顶面
        [0, 1, 5, 4],  # 前面
        [1, 2, 6, 5],  # 右面
        [2, 3, 7, 6],  # 后面
        [3, 0, 4, 7]   # 左面
    ]

    polygons = []
    for face in faces:
        face_points = [points[i] for i in face]
        polygon_points = [f"{x} {y} {z}" for x, y, z in face_points]
        polygon_points.append(polygon_points[0])  # 闭合多边形
        polygon_str = f"(({', '.join(polygon_points)}))"  # 双层括号
        polygons.append(polygon_str)

    return f"POLYHEDRALSURFACE Z ({', '.join(polygons)})"


# =============================================
# 5️⃣ 插入数据库和碰撞检测（复用原有逻辑）
# =============================================
def insert_buildings_to_postgis(conn, building_data: List[Dict]):
    query = """
        INSERT INTO buildings 
        (id, name, tile_url, bounding_volume, refine, properties)
        VALUES (%s, %s, %s, ST_GeomFromEWKT(%s), %s, %s)
    """

    records = []
    for b in building_data:
        bv = b["bounding_volume"]
        ewkt = f"SRID=4978;{bv['to_ewkt']()}"
        tile_url = str(b.get("tile_url")) if b.get("tile_url") else None

        records.append((
            str(uuid.uuid4()),
            "Building",
            tile_url,
            ewkt,
            b.get("refine"),
            json.dumps(b.get("properties", {}))  # 包含瓦片集目录信息
        ))

    with conn.cursor() as cur:
        execute_batch(cur, query, records, page_size=100)
    conn.commit()
    print(f"✅ 成功插入 {len(records)} 条建筑物数据")


def check_collision_in_db(conn, point_ewkt: str) -> List[Tuple]:
    query = """
        SELECT id, tile_url, properties->>'tileset_dir' as source_dir
        FROM buildings
        WHERE ST_Intersects(bounding_volume, ST_GeomFromEWKT(%s))
    """
    with conn.cursor() as cur:
        cur.execute(query, (point_ewkt,))
        return cur.fetchall()


# =============================================
# 🧠 主函数：解析所有瓦片集（根目录+子目录）
# =============================================
def main():
    DB_CONN_STRING = "dbname=nyc user=postgres password=123456 host=localhost port=5432"

    with psycopg2.connect(DB_CONN_STRING) as conn:

        # 配置
        init_tileset(conn)

        # 步骤4：模拟无人机碰撞检测
        drone_lat = -179.800468 # 示例：北京坐标
        drone_lon = -27.927457
        drone_alt = 63.102      # 高度50米
        #x, y, z = wgs84_to_ecef(drone_lat, drone_lon, drone_alt)
        x, y, z = 1370.2255, -726.338, -121.374
        point_ewkt = f"SRID=4978;POINT Z({x} {y} {z})"
        print(f"模拟无人机位置point_ewkt：{point_ewkt}")

        print("\n🚨 正在进行碰撞检测...")
        collisions = check_collision_in_db(conn, point_ewkt)

        if collisions:
            print("⚠️ 警告！与以下瓦片发生碰撞：")
            for c in collisions:
                print(f" - ID: {c[0]}, 瓦片文件: {c[1]}, 来源目录: {c[2]}")
        else:
            print("✅ 安全：当前路径无碰撞风险。")


def init_tileset(conn):
    """初始化瓦片集，解析所有瓦片并插入数据库"""
    ROOT_TILESET_DIR = "../3dtiles"  # 根目录（包含根tileset.json）

    # 步骤1：查找所有 tileset.json 文件（根目录+子目录）
    print(f"🔍 正在查找 {ROOT_TILESET_DIR} 下的所有 tileset.json...")
    all_tileset_files = find_all_tileset_files(ROOT_TILESET_DIR)
    print(f"📋 找到 {len(all_tileset_files)} 个 tileset.json 文件：")
    for file in all_tileset_files:
        print(f"  - {file}")
    # 步骤2：解析每个 tileset.json 并收集所有瓦片包围盒
    all_building_bounds = []
    for tileset_file in all_tileset_files:
        print(f"\n📦 正在解析 {tileset_file}...")
        tileset, tileset_dir = load_tileset_with_path(tileset_file)
        # 提取当前瓦片集的所有瓦片
        tiles_bounds = collect_tileset_bounds(tileset, tileset_dir)
        all_building_bounds.extend(tiles_bounds)
        print(f"📌 从该瓦片集提取 {len(tiles_bounds)} 个瓦片")
    # 步骤3：插入所有瓦片数据到数据库
    print(f"\n💾 总计解析 {len(all_building_bounds)} 个瓦片，准备插入数据库...")

    insert_buildings_to_postgis(conn, all_building_bounds)

    return  all_building_bounds


if __name__ == "__main__":
    main()