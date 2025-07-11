import math


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    使用 Haversine 公式计算两个经纬度点之间的距离。

    参数：
    - lat1, lon1: 第一个点的纬度和经度（单位：度）
    - lat2, lon2: 第二个点的纬度和经度（单位：度）

    返回：
    - 两点之间的距离（单位：米）
    """
    # 地球半径（单位：米）
    R = 6371000

    # 将经纬度从度转换为弧度
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine 公式
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # 计算距离
    distance = R * c
    return distance


# 示例数据
#lat1, lon1 = 30.167928, 120.420556  # 点 A 的纬度和经度
lat1, lon1 = 30.192456, 120.370957  # 点 A 的纬度和经度
lat2, lon2 = 30.193365, 120.371245  # 点 B 的纬度和经度

# 调用函数计算距离
distance = haversine_distance(lat1, lon1, lat2, lon2)

# 输出结果
print(f"两个经纬度点之间的距离为：{distance:.2f} 米")