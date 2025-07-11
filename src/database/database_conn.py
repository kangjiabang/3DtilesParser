# src/database_conn.py

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional, ContextManager
from contextlib import contextmanager


# 数据库连接字符串（可以从 .env 加载）
DB_CONN_STRING = os.getenv("DB_CONN_STRING", "dbname=nyc user=postgres password=123456 host=localhost port=5432")


@contextmanager
def get_db_connection() -> ContextManager[psycopg2.extensions.connection]:
    """
    提供一个数据库连接的上下文管理器。
    使用 with 语法自动处理连接的打开和关闭。
    """
    conn = None
    try:
        conn = psycopg2.connect(DB_CONN_STRING, cursor_factory=RealDictCursor)
        yield conn
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        raise
    finally:
        if conn:
            conn.close()


def get_db_connection_simple():
    """
    简单获取数据库连接（需手动关闭）。
    """
    return psycopg2.connect(DB_CONN_STRING, cursor_factory=RealDictCursor)