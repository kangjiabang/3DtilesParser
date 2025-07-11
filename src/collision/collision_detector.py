# collision_detector.py

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Tuple


def check_collision_in_db(conn, point_x: float, point_y: float, point_z: float) -> List[dict]:
    """
    判断点是否与某栋建筑发生碰撞。
    返回匹配的建筑物列表。
    """
    query = """
        SELECT 
            id, 
            tile_url, 
            properties->>'tileset_dir' AS source_dir,
            height,
            ST_Z(ST_PointN(ST_ExteriorRing(bounding_volume), 1)) AS ground_z
        FROM 
            dk_buildings
        WHERE 
            ST_Intersects(
                ST_Force2D(bounding_volume),
                ST_SetSRID(ST_MakePoint(%(x)s, %(y)s), 4978)
            )
            AND
            %(z)s >= ST_Z(ST_PointN(ST_ExteriorRing(bounding_volume), 1))
            AND
            %(z)s <= ST_Z(ST_PointN(ST_ExteriorRing(bounding_volume), 1)) + height;
    """

    params = {
        "x": point_x,
        "y": point_y,
        "z": point_z
    }

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()