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
        # 处理变换矩阵（避免None）
        if hasattr(tile, 'transform') and tile.transform is not None:
            tile_transform = tile.transform
        else:
            tile_transform = current_transform or [1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1]  # 单位矩阵

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
                # 在 _recursive_collect 函数中修改：
                points = apply_transform(points, tile_transform)
                print("变换后点检查:", points)

                # 检查点是否唯一（转换为元组后检查）
                unique_points = set((tuple(p) for p in points))  # 将每个点转换为元组
                assert len(points) == len(unique_points), "存在重复点！"

            # 生成WKT
            multipolygon_wkt = create_multipolygon_wkt(points)

            # 记录瓦片信息（包含来源目录）
            bounds_info = {
                "bounding_volume": {"to_ewkt": lambda: multipolygon_wkt},
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
    """应用4x4变换矩阵到三维点（处理多维数组）"""
    import numpy as np  # 临时引入处理数组

    # 1. 将变换矩阵转为1D列表（16元素）
    if isinstance(transform_matrix, (list, tuple)):
        # 处理列表/元组（若为2D则展平）
        flat_matrix = []
        for item in transform_matrix:
            if isinstance(item, (list, tuple, np.ndarray)):
                flat_matrix.extend(list(item))
            else:
                flat_matrix.append(item)
    elif isinstance(transform_matrix, np.ndarray):
        # 处理NumPy数组（展平为1D）
        flat_matrix = transform_matrix.flatten().tolist()
    else:
        raise TypeError(f"不支持的变换矩阵类型：{type(transform_matrix)}")

    # 2. 补全为16元素（4x4矩阵）
    flat_matrix = flat_matrix[:16]  # 截断过长部分
    flat_matrix += [0.0] * (16 - len(flat_matrix))  # 补全过短部分
    # 设置单位矩阵默认值（若未指定）
    if flat_matrix[0] == 0: flat_matrix[0] = 1.0
    if flat_matrix[5] == 0: flat_matrix[5] = 1.0
    if flat_matrix[10] == 0: flat_matrix[10] = 1.0
    if flat_matrix[15] == 0: flat_matrix[15] = 1.0

    # 3. 矩阵乘法（纯Python实现）
    def multiply(matrix, x, y, z, w=1.0):
        x = float(x)
        y = float(y)
        z = float(z)

        x_new = matrix[0] * x + matrix[4] * y + matrix[8] * z + matrix[12] * w
        y_new = matrix[1] * x + matrix[5] * y + matrix[9] * z + matrix[13] * w
        z_new = matrix[2] * x + matrix[6] * y + matrix[10] * z + matrix[14] * w
        w_new = matrix[3] * x + matrix[7] * y + matrix[11] * z + matrix[15] * w

        if abs(w_new) < 1e-10:
            w_new = 1e-10

        return (
            float(x_new / w_new),
            float(y_new / w_new),
            float(z_new / w_new)
        )

    # 4. 确保点坐标为标量
    points = [(float(x), float(y), float(z)) for x, y, z in points]

    return [multiply(flat_matrix, x, y, z) for x, y, z in points]



def clean_points(raw_points):
    """清洗点坐标（确保三维坐标完整）"""
    cleaned = []
    for p in raw_points:
        # 强制提取三维坐标（处理可能的数组或缺失值）
        try:
            x = float(p[0]) if not hasattr(p[0], '__iter__') else float(p[0].item())
            y = float(p[1]) if not hasattr(p[1], '__iter__') else float(p[1].item())
            z = float(p[2]) if not hasattr(p[2], '__iter__') else float(p[2].item())
            cleaned.append((x, y, z))
        except (IndexError, AttributeError) as e:
            print(f"清洗点{p}失败：{e}")
            return []  # 若有无效点，返回空列表避免后续错误
    return cleaned


def create_multipolygon_wkt(points):
    """生成严格的三维MULTIPOLYGON Z，确保每个点包含Z坐标"""
    points = clean_points(points)

    # 验证点的有效性（必须是8个三维点）
    if len(points) != 8:
        print(f"错误：需8个顶点，实际为{len(points)}个")
        return None
    for i, p in enumerate(points):
        if len(p) != 3 or any(not isinstance(coord, (int, float)) for coord in p):
            print(f"错误：点{i} {p}不是有效的三维坐标")
            return None

    # 重新定义6个面的顶点索引（确保每个面是独立的三维四边形）
    faces = [
        [0, 1, 2, 3],  # 底面（z最小）
        [4, 5, 6, 7],  # 顶面（z最大）
        [0, 1, 5, 4],  # 前面
        [1, 2, 6, 5],  # 右面
        [2, 3, 7, 6],  # 后面
        [3, 0, 4, 7]  # 左面
    ]

    polygons = []
    for face_idx, face in enumerate(faces):
        try:
            # 提取面的4个顶点（强制保留Z坐标）
            face_points = [points[i] for i in face]
            # 检查是否为三维点
            for p_idx, p in enumerate(face_points):
                if len(p) != 3:
                    print(f"错误：面{face_idx}的点{p_idx} {p}不是三维坐标")
                    return None

            # 闭合多边形（添加第一个点，确保Z坐标一致）
            face_points.append(face_points[0])

            # 生成坐标字符串（显式保留Z坐标，即使为0）
            coord_str = []
            for x, y, z in face_points:
                # 格式化坐标为字符串，避免科学计数法导致解析错误
                coord_str.append(f"{x:.6f} {y:.6f} {z:.6f}")
            polygons.append(f"(({', '.join(coord_str)}))")

        except IndexError as e:
            print(f"顶点索引错误：{e}，面{face_idx}的索引{face}无效")
            return None

    # 构造WKT（严格指定Z）
    wkt_str = f"MULTIPOLYGON Z ({', '.join(polygons)})"

    print(f"🔧 生成的 MULTIPOLYGON Z WKT: {wkt_str}")

    # 验证几何有效性
    try:
        geom = wkt.loads(wkt_str)
        if not geom.is_valid:
            print(f"无效几何: {explain_validity(geom)}")
            # 强制PostGIS修复三维几何
            #return f"SRID=4978;ST_Force3D(ST_MakeValid({wkt_str}))"
        return f"{wkt_str}"
    except Exception as e:
        print(f"WKT解析失败: {e}，原始WKT: {wkt_str[:100]}...")
        return None


# =============================================
# 5️⃣ 插入数据库和碰撞检测（复用原有逻辑）
# =============================================
def insert_buildings_to_postgis(conn, building_data: List[Dict]):
    query = """
        INSERT INTO dk_buildings 
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
        FROM dk_buildings
        WHERE ST_3DIntersects(bounding_volume, ST_GeomFromEWKT(%s))
    """
    with conn.cursor() as cur:
        cur.execute(query, (point_ewkt,))
        return cur.fetchall()

# 修改数据库表结构建议
def init_db_schema(conn):
    """初始化数据库表结构（使用MULTIPOLYGON Z）"""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dk_buildings (
            id UUID PRIMARY KEY,
            name TEXT,
            tile_url TEXT,
            bounding_volume GEOMETRY(MULTIPOLYGONZ, 4978),  -- 明确指定类型
            refine TEXT,
            properties JSONB
        );
        CREATE INDEX IF NOT EXISTS idx_buildings_geom ON dk_buildings USING GIST(bounding_volume);
        """)
    conn.commit()

# =============================================
# 🧠 主函数：解析所有瓦片集（根目录+子目录）
# =============================================
def main():
    DB_CONN_STRING = "dbname=nyc user=postgres password=123456 host=localhost port=5432"

    with psycopg2.connect(DB_CONN_STRING) as conn:

        # 初始化数据库表结构
        #init_db_schema(conn)
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