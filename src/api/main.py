# api/main.py
import os
import sys
import uvicorn
from fastapi import FastAPI, Query, HTTPException
from typing import Optional

# 导入数据库连接工具
from src.database.database_conn import get_db_connection
# 导入业务逻辑模块
from src.collision.collision_detector import check_collision_in_db
from src.core.tilesParserSinglePoligonZ import init_tileset
from src.utils.coordinate import wgs84_to_ecef
# 获取当前文件所在目录的上一级目录
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

app = FastAPI(title="3D Building Collision Detector")


@app.post("/init-data")
async def initialize_data():
    """
    触发瓦片集解析并插入数据库的操作。
    """
    try:
        with get_db_connection() as conn:
            init_tileset(conn)

        return {"status": "success", "result": "Data initialized successfully."}

    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/collision")
async def detect_collision(
    lon: float = Query(..., description="经度（WGS84）"),
    lat: float = Query(..., description="纬度（WGS84）"),
    alt: float = Query(..., description="海拔高度（米）"),
):
    """
    根据传入的 WGS84 经纬高判断是否与建筑物发生碰撞。
    """
    try:
        # 转换为 ECEF 坐标
        x, y, z = wgs84_to_ecef(lat, lon, alt)

        print(f"ECEF: {x}, {y}, {z}")

        # 使用原有逻辑进行碰撞检测
        with get_db_connection() as conn:
            result = check_collision_in_db(conn, x, y, z)

        return {
            "status": "success",
            "point": {"x": x, "y": y, "z": z},
            "collisions": result
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}


def main():
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    main()