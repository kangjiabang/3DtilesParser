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


def box_center(box):
    """从 boundingVolume.box 提取中心点"""
    return np.array(box[0:3], dtype=np.float64)


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
    """递归处理 tile"""
    if parent_transform is None:
        parent_transform = np.eye(4)

    if results is None:
        results = []

    # 获取当前 tile 的变换矩阵
    current_transform = parent_transform.copy()
    if 'transform' in tile:
        local_matrix = matrix_from_column_major(tile['transform'])
        current_transform = current_transform @ local_matrix

    # 处理包围盒
    if 'boundingVolume' in tile and 'box' in tile['boundingVolume']:
        box = tile['boundingVolume']['box']
        local_center = box_center(box)
        world_center = apply_transform(local_center, current_transform)

        results.append({
            'path': path,
            'level': level,
            'uri': tile.get('content', {}).get('uri', 'N/A'),
            'local_center': local_center.tolist(),
            'world_center': world_center.tolist(),
        })

    # 递归处理 children
    if 'children' in tile and isinstance(tile['children'], list):
        for i, child in enumerate(tile['children']):
            child_path = f"{path}/children[{i}]"
            process_tile(child, current_transform, level + 1, results, child_path)

    return results


if __name__ == "__main__":
    # 替换为你自己的 tileset.json 路径（支持 URL 或本地路径）
    tileset_path = "../../3dtiles/Tile_+000_+005/tileset.json"  # 或者类似 "http://yourdomain.com/tileset.json"

    print("Loading tileset...")
    tileset = load_tileset(tileset_path)

    print("Processing tiles...")
    all_centers = process_tile(tileset["root"])

    print("\nWorld Coordinates of All Tile Centers:")
    for idx, item in enumerate(all_centers):
        print(f"[{idx}] {item['path']}")
        print(f"  URI: {item['uri']}")
        print(f"  Level: {item['level']}")
        print(f"  Local Center: {item['local_center']}")
        print(f"  World Center: {item['world_center']}")
        print("-" * 50)

    # 可选：保存结果到文件
    with open("tile_centers_output.json", "w", encoding="utf-8") as f:
        json.dump(all_centers, f, indent=2)
    print("\n✅ 所有 tile 中心坐标已保存至: tile_centers_output.json")

