import numpy as np

# 定义数值
cx, cy, cz = -2231.1857, 2084.7526, -131.8731
x_half = 73.2738
y_half = 128.1197
z_half = 12.0119

# 所有角点 Z 值（因为只有 z_axis 有 Z 分量）
z_values = [
    cz - z_half,  # 底面
    cz - z_half,
    cz - z_half,
    cz - z_half,
    cz + z_half,  # 顶面
    cz + z_half,
    cz + z_half,
    cz + z_half
]

ground_z = min(z_values)
top_z = max(z_values)
height = top_z - ground_z

print(f"最低点 Z: {ground_z:.4f}")
print(f"最高点 Z: {top_z:.4f}")
print(f"建筑物高度: {height:.4f} 米")