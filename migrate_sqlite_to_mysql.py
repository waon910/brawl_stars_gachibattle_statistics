import argparse
import sqlite3
from db import get_connection

TABLES = [
    'players',
    '_modes',
    '_maps',
    '_ranks',
    '_brawlers',
    'brawler_used_ranks',
    'rank_logs',
    'battle_logs',
    'win_lose_logs'
]

def migrate(sqlite_path: str) -> None:
    src = sqlite3.connect(sqlite_path)
    dst = get_connection()
    s_cur = src.cursor()
    d_cur = dst.cursor()
    for table in TABLES:
        rows = s_cur.execute(f'SELECT * FROM {table}').fetchall()
        if not rows:
            continue
        placeholders = ','.join(['%s'] * len(rows[0]))
        for row in rows:
            d_cur.execute(f'REPLACE INTO {table} VALUES ({placeholders})', row)
    dst.commit()
    src.close()
    dst.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SQLiteデータをMySQLへ移行')
    parser.add_argument('--sqlite', default='brawl_stats.db', help='SQLiteファイルパス')
    args = parser.parse_args()
    migrate(args.sqlite)
