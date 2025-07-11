# src/utils/coordinate.py

import math
from pyproj import Transformer

"""
将 WGS84 坐标转换为 ECEF 坐标
"""
def wgs84_to_ecef(lon: float, lat: float, alt: float) -> tuple:
    # 创建转换器：从 WGS84 (EPSG:4326) 到 ECEF (EPSG:4978)
    transformer_inv = Transformer.from_crs("epsg:4326", "epsg:4978", always_xy=True)

    # 转换
    x_i, y_i, z_i = transformer_inv.transform(lon, lat, alt)

    print(f"x={x_i:.6f}米, y={y_i:.6f}米, z={z_i:.2f}米")
    return x_i, y_i, z_i




"""
将 ECEF 坐标转换为 WGS84 坐标
"""
def ecef_to_wgs84(x, y, z):
    # 创建转换器：从 ECEF (EPSG:4978) 到 WGS84 (EPSG:4326)
    transformer = Transformer.from_crs("epsg:4978", "epsg:4326", always_xy=True)

    # 转换
    lon, lat, alt = transformer.transform(x, y, z)

    print(f"纬度={lat:.6f}°, 经度={lon:.6f}°, 高度={alt:.2f}米")

    return lon, lat, alt




# 实例：一个真实的 ECEF 坐标（单位：米）
# x = 1287681.40
# y = -4716944.12
# z = 4083339.32
#
# # 转换
# lon, lat, alt = ecef_to_wgs84(x, y, z)
#
# print(f"纬度={lat:.6f}°, 经度={lon:.6f}°, 高度={alt:.2f}米")
#
# # 创建转换器：从 ECEF (EPSG:4978) 到 WGS84 (EPSG:4326)
#
# # 转换
# x_i, y_i, z_i = wgs84_to_ecef(lon, lat, alt)
#
# print(f"x={x_i:.6f}°, y={y_i:.6f}°, z={z_i:.2f}米")


from pyproj import Transformer

# # 来自 tileset.json root boundingVolume.box 的前三个值
# x = -2601.133566245903
# y = -2518.235788370017
# z = -51.8022186094895
#
# # 创建 ECEF 到 WGS84 的转换器，并设置 always_xy=True（推荐）
# transformer = Transformer.from_crs("epsg:4978", "epsg:4326", always_xy=True)
#
# # 转换 ECEF → WGS84
# lon, lat, alt = transformer.transform(x, y, z)
#
# print(f"参考点经纬高：")
# print(f"纬度={lat:.6f}°, 经度={lon:.6f}°, 高度={alt:.2f}米")

transformer = Transformer.from_crs("epsg:4978", "epsg:4326", always_xy=True)


#cx,cy,cz = -2789740.704430894, 4760053.504586963, 3189296.4507276197
cx,cy,cz = -2793868.430852 ,4759703.469316 ,3186210.37923

lon, lat, alt = transformer.transform(cx, cy, cz)

print(f"ECEF 坐标 ({cx:.6f}, {cy:.6f}, {cz:.6f})")
print(f"WGS84 坐标：纬度={lat:.6f}°, 经度={lon:.6f}°, 高度={alt:.2f}米")



