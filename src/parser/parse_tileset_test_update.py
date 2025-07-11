import json
import numpy as np


def load_tileset(file_path):
    """加载 tileset JSON 文件"""
    if file_path.startswith("http"):
        import requests
        response = requests.get(file_path)
        return response.json()
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)


def box_center_and_corners(box):
    """精确的 box 到角点转换，考虑所有三个轴"""
    center = np.array(box[0:3], dtype=np.float64)
    x_axis = np.array(box[3:6], dtype=np.float64)
    y_axis = np.array(box[6:9], dtype=np.float64)
    z_axis = np.array(box[9:12], dtype=np.float64)

    all_corners = [
        center - x_axis - y_axis - z_axis,
        center + x_axis - y_axis - z_axis,
        center + x_axis + y_axis - z_axis,
        center - x_axis + y_axis - z_axis,

        center - x_axis - y_axis + z_axis,
        center + x_axis - y_axis + z_axis,
        center + x_axis + y_axis + z_axis,
        center - x_axis + y_axis + z_axis,
    ]
    # 构造全部 8 个角点
    # bottom_corners = [
    #     center - x_axis - y_axis,  # 左下
    #     center + x_axis - y_axis,  # 右下
    #     center + x_axis + y_axis,  # 右上
    #     center - x_axis + y_axis   # 左上
    # ]
    # top_corners = [
    #     corner + 2 * z_axis for corner in bottom_corners
    # ]
    #
    # all_corners = bottom_corners + top_corners
    return center, all_corners  # 返回全部 8 个角点


def apply_transform(point, transform_matrix):
    """将局部坐标点应用变换矩阵"""
    # 构造齐次坐标 [x, y, z, 1]
    homogeneous_point = np.append(point, 1.0)
    # 应用变换
    transformed = np.dot(transform_matrix, homogeneous_point)
    # 返回 xyz 部分
    return transformed[:3]


def matrix_from_column_major(array):
    """将列优先数组转为 4x4 矩阵"""
    return np.array(array, dtype=np.float64).reshape((4, 4), order='F')


def process_tile(tile, parent_transform=None, level=0, results=None, path="root"):
    """递归处理 tile，只处理没有 children 的 tile"""
    if parent_transform is None:
        parent_transform = np.eye(4)

    if results is None:
        results = []

    # 获取当前 tile 的变换矩阵
    current_transform = parent_transform.copy()
    if 'transform' in tile:
        local_matrix = matrix_from_column_major(tile['transform'])
        current_transform = current_transform @ local_matrix

    # 只处理没有 children 的 tile
    if 'children' not in tile or not tile['children']:
        if 'boundingVolume' in tile and 'box' in tile['boundingVolume']:
            box = tile['boundingVolume']['box']
            local_center, local_corners = box_center_and_corners(box)
            world_center = apply_transform(local_center, current_transform)

            # 转换所有角点到世界坐标
            world_corners = [apply_transform(corner, current_transform) for corner in local_corners]

            # 提取 Z 值用于高度计算
            z_values = [corner[2] for corner in local_corners]
            height = max(z_values) - min(z_values)

            # 创建底面多边形（闭合环，重复第一个点）
            bottom_corners = world_corners[:4]
            polygon_z = [corner.tolist() for corner in bottom_corners] + [bottom_corners[0].tolist()]

            results.append({
                'path': path,
                'level': level,
                'uri': tile.get('content', {}).get('uri', 'N/A'),
                'local_center': local_center.tolist(),
                'world_center': world_center.tolist(),
                'polygon_z': polygon_z,  # 底面多边形坐标
                'area': calculate_polygon_area(polygon_z),  # 计算底面面积
                'height': height  # 新增高度字段
            })

    # 递归处理 children（即使不处理当前 tile，也要继续递归）
    if 'children' in tile and isinstance(tile['children'], list):
        for i, child in enumerate(tile['children']):
            child_path = f"{path}/children[{i}]"
            process_tile(child, current_transform, level + 1, results, child_path)

    return results


def calculate_polygon_area(polygon):
    """计算多边形面积（使用鞋带公式）"""
    if len(polygon) < 3:
        return 0.0

    # 只使用 x,y 坐标计算平面面积
    area = 0.0
    n = len(polygon)
    for i in range(n):
        x_i, y_i, _ = polygon[i]
        x_j, y_j, _ = polygon[(i + 1) % n]
        area += (x_i * y_j) - (x_j * y_i)

    return abs(area) / 2.0


if __name__ == "__main__":
    # 替换为你自己的 tileset.json 路径（支持 URL 或本地路径）
    tileset_path = "../../3dtiles/Tile_+000_+005/tileset.json"  # 或者类似 "http://yourdomain.com/tileset.json"

    print("Loading tileset...")
    tileset = load_tileset(tileset_path)

    print("Processing tiles...")
    all_centers = process_tile(tileset["root"])

    print("\nWorld Coordinates of All Tile Centers and Polygons:")
    for idx, item in enumerate(all_centers):
        print(f"[{idx}] {item['path']}")
        print(f"  URI: {item['uri']}")
        print(f"  Level: {item['level']}")
        print(f"  Local Center: {item['local_center']}")
        print(f"  World Center: {item['world_center']}")
        print(f"  Polygon Z Area: {item['area']:.2f} m²")
        print("  Polygon Z Coordinates:")
        for coord in item['polygon_z']:
            print(f"    {coord}")
        print("-" * 50)

    # 可选：保存结果到文件
    with open("tile_polygons_output.json", "w", encoding="utf-8") as f:
        json.dump(all_centers, f, indent=2)
    print("\n✅ 所有 tile 中心坐标和底面多边形已保存至: tile_polygons_output.json")