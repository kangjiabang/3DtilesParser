# src/database_conn.py

import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from typing import Optional, ContextManager
from contextlib import contextmanager


# 数据库连接字符串（可以从 .env 加载）
#DB_CONN_STRING = os.getenv("DB_CONN_STRING", "dbname=nyc user=postgres password=123456 host=localhost port=5432")
# 获取当前环境，默认为 development
env = os.getenv("ENV", "development")

# 构造要加载的 .env 文件路径,上两级目录
dotenv_path_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dotenv_env_path = os.path.join(dotenv_path_dir, f".env.{env}")
print(f"加载环境变量文件: {dotenv_env_path}")
# 加载环境变量
load_dotenv(dotenv_env_path)

# 从环境变量中读取数据库配置
db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# 构建连接字符串（过滤掉 None 值）
DB_CONN_STRING = " ".join([f"{k}={v}" for k, v in db_config.items() if v is not None])

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