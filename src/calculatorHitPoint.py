import math


def ecef_to_wgs84(x, y, z):
    """将ECEF坐标转换为WGS84经纬度和高度"""
    a = 6378137.0  # WGS84长半轴
    f = 1 / 298.257223563  # WGS84扁率
    b = a * (1 - f)  # WGS84短半轴
    e_sq = 2 * f - f ** 2  # 第一偏心率平方
    e_prime_sq = e_sq / (1 - e_sq)  # 第二偏心率平方

    # 计算经度
    lon = math.atan2(y, x)

    # 计算纬度（使用Bowring算法迭代）
    p = math.sqrt(x ** 2 + y ** 2)
    theta = math.atan2(z * a, p * b)

    phi = math.atan2(
        z + e_prime_sq * b * math.sin(theta) ** 3,
        p - e_sq * a * math.cos(theta) ** 3
    )

    # 计算高度
    N = a / math.sqrt(1 - e_sq * math.sin(phi) ** 2)
    h = p / math.cos(phi) - N

    # 转换为度
    lat = math.degrees(phi)
    lon = math.degrees(lon)

    return lat, lon, h


# 使用建筑物中心点坐标
x, y, z = 1370.2255, -726.338, -121.374

lat, lon, h = ecef_to_wgs84(x, y, z)
print(f"碰撞点经纬度: 纬度={lat:.8f}°, 经度={lon:.8f}°, 高度={h:.3f}米")