import os

import mysql.connector
from sqlalchemy import create_engine

from .settings import load_environment


def get_connection():
    load_environment()
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "brawl_stats"),
        autocommit=True,
    )


def get_engine():
    load_environment()
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    host = os.getenv("MYSQL_HOST", "localhost")
    database = os.getenv("MYSQL_DB", "brawl_stats")
    url = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"
    return create_engine(url)
