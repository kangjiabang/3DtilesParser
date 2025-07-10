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
    """
    将 WGS84 坐标 (lat, lon, alt) 转换为 ECEF 坐标 (x, y, z)
    :param lat_deg: 纬度（度）
    :param lon_deg: 经度（度）
    :param alt_m: 海拔高度（米）
    :return: x, y, z 地心坐标（米）
    """
    # WGS84 参数
    a = 6378137.0          # 长半轴
    f = 1 / 298.257223563  # 扁率
    b = a * (1 - f)        # 短半轴
    e_sq = 2*f - f**2      # 第一偏心率平方

    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)

    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    sin_lon = math.sin(lon)
    cos_lon = math.cos(lon)

    N = a / math.sqrt(1 - e_sq * sin_lat ** 2)
    x = (N + alt_m) * cos_lat * cos_lon
    y = (N + alt_m) * cos_lat * sin_lon
    z = ((b**2 / a**2) * N + alt_m) * sin_lat

    return x, y, z

# =============================================
# 1️⃣ 加载 tileset
# =============================================
def load_tileset(tileset_path: str) -> TileSet:
    path = Path(tileset_path)
    with path.open('r') as f:
        data = json.load(f)
    return TileSet.from_dict(data)


# =============================================
# 2️⃣ 提取所有 tile 的包围盒信息
# =============================================
def collect_building_bounds(tile, base_transform=None) -> List[Dict]:
    bounds_list = []

    if hasattr(tile, 'transform'):
        transform = tile.transform
    else:
        transform = base_transform

    bv = tile.bounding_volume

    if bv is not None:
        # 获取包围盒的中心、轴对齐的半径（x, y, z 半长）
        box = bv._box  # 这是一个长度为 12 的列表 [cx, cy, cz, dx, dy, dz, ...] ?

        # 注意：box[0:3] 是包围盒中心点 (cx, cy, cz)
        #       box[3], box[7], box[11] 分别是 x, y, z 方向的半长（即半宽）

        cx, cy, cz = box[0], box[1], box[2]
        hx = box[3]  # x方向半长
        hy = box[7]  # y方向半长
        hz = box[11] # z方向半长

        # 构造包围盒的8个顶点（局部坐标）
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

        # 如果有 transform 矩阵，应用变换
        if transform is not None:
            points = apply_transform(points, transform)

        # 转换为 POLYHEDRALSURFACEZ 的 EWKT 字符串格式
        polyhedron_wkt = create_polyhedron_wkt(points)

        bounds_info = {
            "bounding_volume": {
                "to_ewkt": lambda: polyhedron_wkt  # 模拟旧版 .to_ewkt() 接口
            },
            "tile_url": tile.content_uri if hasattr(tile, 'content_uri') and tile.content_uri else None,
            "refine": tile._refine,
            "properties": {}
        }
        bounds_list.append(bounds_info)

    for child in tile.children:
        bounds_list.extend(collect_building_bounds(child, transform))

    return bounds_list

def apply_transform(points, transform_matrix):
    """
    应用 4x4 变换矩阵到一组三维点上
    :param points: list of (x, y, z)
    :param transform_matrix: list of 16 float values (column-major) or shorter (e.g. translation only)
    :return: transformed points
    """

    # 如果 transform_matrix 不是 16 个元素，则构造为一个单位矩阵 + 平移向量
    if len(transform_matrix) < 16:
        identity = [
            1, 0, 0, 0,
            0, 1, 0, 0,
            0, 0, 1, 0,
            0, 0, 0, 1
        ]
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
    """
    清洗原始points数据，提取有效的(x, y, z)三元组
    :param raw_points: 原始points，格式为[(array, array, array), ...]
    :return: 清洗后的points，格式为[(x1,y1,z1), ..., (x8,y8,z8)]
    """
    cleaned = []
    for p in raw_points:
        # 假设每个array的前3个元素是有效的x, y, z（根据实际数据调整）
        x = p[0][0]  # 取第一个数组的第一个元素作为x
        y = p[1][0]  # 取第二个数组的第一个元素作为y
        z = p[2][0]  # 取第三个数组的第一个元素作为z
        cleaned.append((x, y, z))
    return cleaned

def create_polyhedron_wkt(points):

    points = clean_points(points)
    """
    生成符合PostGIS规范的POLYHEDRALSURFACE Z WKT
    :param points: 8个顶点的列表，格式为[(x1,y1,z1), ..., (x8,y8,z8)]
    :return: 正确格式的WKT字符串
    """
    # 定义立方体的6个面（每个面包含4个顶点索引）
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
        # 获取当前面的4个顶点
        face_points = [points[i] for i in face]
        # 格式化每个点为 "x y z"
        polygon_points = [f"{x} {y} {z}" for x, y, z in face_points]
        # 闭合多边形（首尾点必须相同，确保多边形闭合）
        polygon_points.append(polygon_points[0])
        # 每个面必须用双层括号((...))包裹（关键修正！）
        polygon_str = f"(({', '.join(polygon_points)}))"
        polygons.append(polygon_str)

    # 组合所有面，外层用单括号包裹
    result = f"POLYHEDRALSURFACE Z ({', '.join(polygons)})"
    print(f"🔧 修正后的 POLYHEDRALSURFACEZ WKT: {result}")
    return result

# =============================================
# 3️⃣ 插入建筑物数据到 PostGIS
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
        ewkt = f"SRID=4978;{bv['to_ewkt']()}"  # 调用 lambda 获取 WKT 字符串

        # 确保 tile_url 是字符串
        tile_url = str(b.get("tile_url")) if b.get("tile_url") else None

        records.append((
            str(uuid.uuid4()),
            "Building",
            tile_url,
            ewkt,
            b.get("refine"),
            json.dumps(b.get("properties", {}))
        ))

    with conn.cursor() as cur:
        execute_batch(cur, query, records, page_size=100)
    conn.commit()
    print(f"✅ 成功插入 {len(records)} 条建筑物数据")


# =============================================
# 4️⃣ 检测无人机位置是否碰撞
# =============================================
def check_collision_in_db(conn, point_ewkt: str) -> List[Tuple]:
    query = """
        SELECT id, tile_url
        FROM buildings
        WHERE ST_Intersects(bounding_volume, ST_GeomFromEWKT(%s))
    """
    with conn.cursor() as cur:
        cur.execute(query, (point_ewkt,))
        return cur.fetchall()


# =============================================
# 🧠 主函数：完整流程串联
# =============================================
def main():

    building_bounds = init_buildings()

    DB_CONN_STRING = "dbname=nyc user=postgres password=123456 host=localhost port=5432"
    # --- 3. 连接数据库并入库 ---
    print("💾 正在连接数据库...")
    with psycopg2.connect(DB_CONN_STRING) as conn:

        #print("📥 正在导入建筑物数据...")
        #insert_buildings_to_postgis(conn, building_bounds)

        # --- 4. 模拟无人机位置并检测碰撞 ---
        drone_lat = 39.9042  # 示例坐标
        drone_lon = 116.4074
        drone_alt = 50       # 米
        drone_ecef = wgs84_to_ecef(drone_lat, drone_lon, drone_alt)
        x, y, z = drone_ecef
        point_ewkt = f"SRID=4978;POINT Z({x} {y} {z})"

        print("🚨 正在进行碰撞检测...")
        collisions = check_collision_in_db(conn, point_ewkt)

        # --- 5. 输出结果 ---
        if collisions:
            print("⚠️ 警告！与以下建筑物发生碰撞：")
            for c in collisions:
                print(f" - 建筑物 ID: {c[0]}, 文件: {c[1]}")
        else:
            print("✅ 安全：当前路径无碰撞风险。")


def init_buildings():
    # --- 配置 ---
    TILESET_PATH = "../3dtiles/tileset.json"

    # --- 1. 读取 tileset ---
    print("📦 正在加载 tileset...")
    tileset = load_tileset(TILESET_PATH)
    # --- 2. 提取建筑物包围盒 ---
    print("🔍 正在解析建筑物包围盒...")
    building_bounds = collect_building_bounds(tileset.root_tile)
    return  building_bounds


if __name__ == "__main__":
    main()