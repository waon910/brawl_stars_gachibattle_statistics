import os
import mysql.connector
from dotenv import load_dotenv

def get_connection():
    load_dotenv(dotenv_path=".env.local")
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "brawl_stats"),
        autocommit=True,
    )
