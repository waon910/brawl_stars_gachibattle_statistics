from datetime import date, datetime, timedelta

from db import get_connection, get_engine

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    st_autorefresh = None

# シーズン計算用の基準値
BASE_SEASON = 40
BASE_START = date(2025, 7, 3)


def first_thursday(year: int, month: int) -> date:
    """指定した年月の第1木曜日を返す"""
    d = date(year, month, 1)
    while d.weekday() != 3:  # 0=Mon, 3=Thu
        d += timedelta(days=1)
    return d


def season_from_date(d: date) -> int:
    """日付からシーズン番号を求める"""
    diff = (d.year - BASE_START.year) * 12 + (d.month - BASE_START.month)
    if d < first_thursday(d.year, d.month):
        diff -= 1
    return BASE_SEASON + diff


def season_range(season: int) -> tuple[date, date]:
    """シーズン番号から開始日と次シーズン開始日を取得"""
    diff = season - BASE_SEASON
    year = BASE_START.year + (BASE_START.month - 1 + diff) // 12
    month = (BASE_START.month - 1 + diff) % 12 + 1
    start = first_thursday(year, month)
    next_diff = diff + 1
    next_year = BASE_START.year + (BASE_START.month - 1 + next_diff) // 12
    next_month = (BASE_START.month - 1 + next_diff) % 12 + 1
    next_start = first_thursday(next_year, next_month)
    return start, next_start


def load_seasons() -> list[int]:
    """DBに存在するシーズン一覧を取得"""
    with get_engine().connect() as conn:
        dates = pd.read_sql_query(
            "SELECT DISTINCT SUBSTRING(id,1,8) AS date FROM battle_logs", conn
        )["date"]
    seasons = {
        season_from_date(datetime.strptime(d, "%Y%m%d").date()) for d in dates
    }
    return sorted(seasons)

def load_modes():
    with get_engine().connect() as conn:
        return pd.read_sql_query("SELECT id, name_ja FROM _modes ORDER BY id", conn)

def load_maps(mode_id):
    with get_engine().connect() as conn:
        return pd.read_sql_query(
            "SELECT id, name_ja FROM _maps WHERE mode_id=%s ORDER BY id",
            conn,
            params=(mode_id,),
        )

def load_ranks():
    with get_engine().connect() as conn:
        return pd.read_sql_query("SELECT id, name_ja FROM _ranks ORDER BY id", conn)

def brawler_usage(
    season_id=None, rank_id=None, mode_id=None, map_id=None
) -> pd.DataFrame:
    """指定した階層でのキャラ使用率を集計する"""
    query = (
        "SELECT b.name_ja AS brawler, COUNT(*) AS count "
        "FROM ("
        " SELECT DISTINCT win_brawler_id AS brawler_id, battle_log_id FROM win_lose_logs"
        " UNION ALL"
        " SELECT DISTINCT lose_brawler_id AS brawler_id, battle_log_id FROM win_lose_logs"
        ") wl "
        "JOIN battle_logs bl ON wl.battle_log_id = bl.id "
        "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
        "JOIN _maps m ON rl.map_id = m.id "
        "JOIN _brawlers b ON wl.brawler_id = b.id WHERE 1=1"
    )
    params: list = []
    if season_id is not None:
        start, next_start = season_range(season_id)
        query += " AND SUBSTRING(bl.id,1,8) >= %s AND SUBSTRING(bl.id,1,8) < %s"
        params.extend([start.strftime("%Y%m%d"), next_start.strftime("%Y%m%d")])
    if rank_id is not None:
        query += " AND rl.rank_id=%s"
        params.append(rank_id)
    if mode_id is not None:
        query += " AND m.mode_id=%s"
        params.append(mode_id)
    if map_id is not None:
        query += " AND rl.map_id=%s"
        params.append(map_id)
    query += " GROUP BY b.name_ja"
    with get_engine().connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    total = df["count"].sum()
    if total > 0:
        df["usage_rate"] = df["count"] / total * 100
    else:
        df["usage_rate"] = 0
    return df.sort_values("usage_rate", ascending=False)

def brawler_win_rate(season_id=None, rank_id=None, mode_id=None, map_id=None):
    """指定した階層でのキャラ勝率を集計する"""
    base = (
        "FROM win_lose_logs w "
        "JOIN battle_logs bl ON w.battle_log_id = bl.id "
        "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
        "JOIN _maps m ON rl.map_id = m.id WHERE 1=1"
    )
    params: list = []
    if season_id is not None:
        start, next_start = season_range(season_id)
        base += " AND SUBSTRING(bl.id,1,8) >= %s AND SUBSTRING(bl.id,1,8) < %s"
        params.extend([start.strftime("%Y%m%d"), next_start.strftime("%Y%m%d")])
    if rank_id is not None:
        base += " AND rl.rank_id=%s"
        params.append(rank_id)
    if mode_id is not None:
        base += " AND m.mode_id=%s"
        params.append(mode_id)
    if map_id is not None:
        base += " AND rl.map_id=%s"
        params.append(map_id)
    with get_engine().connect() as conn:
        wins = pd.read_sql_query(
            "SELECT w.win_brawler_id AS brawler_id, COUNT(*) AS wins "
            + base
            + " GROUP BY w.win_brawler_id",
            conn,
            params=params,
        )
        losses = pd.read_sql_query(
            "SELECT w.lose_brawler_id AS brawler_id, COUNT(*) AS losses "
            + base
            + " GROUP BY w.lose_brawler_id",
            conn,
            params=params,
        )
        brawlers = pd.read_sql_query("SELECT id, name_ja FROM _brawlers", conn)
    df = pd.merge(wins, losses, on="brawler_id", how="outer").fillna(0)
    df["total"] = df["wins"] + df["losses"]
    df["win_rate"] = (df["wins"] / df["total"]) * 100
    df = df.merge(brawlers, left_on="brawler_id", right_on="id").drop("id", axis=1)
    return df.rename(columns={"name_ja": "brawler"}).sort_values("win_rate", ascending=False)

def battle_counts(season_id=None, rank_id=None, mode_id=None, map_id=None):
    """各階層での対戦数を取得する"""
    if season_id is not None:
        start, next_start = season_range(season_id)
        season_cond = "SUBSTRING(id,1,8) >= %s AND SUBSTRING(id,1,8) < %s"
        season_params = [start.strftime("%Y%m%d"), next_start.strftime("%Y%m%d")]
    else:
        season_cond = "1=1"
        season_params = []

    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(
            f"SELECT COUNT(*) FROM battle_logs bl WHERE {season_cond}",
            season_params,
        )
        overall = cursor.fetchone()[0]

        cond = season_cond
        params = season_params

        if rank_id is not None:
            cond_rank = f"{cond} AND rl.rank_id=%s"
            params_rank = params + [rank_id]
            cursor.execute(
                f"SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
                f"WHERE {cond_rank}",
                params_rank,
            )
            rank_total = cursor.fetchone()[0]
        else:
            cond_rank = cond
            params_rank = params
            rank_total = overall

        if mode_id is not None:
            cond_mode = f"{cond_rank} AND m.mode_id=%s"
            params_mode = params_rank + [mode_id]
            cursor.execute(
                f"SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
                "JOIN _maps m ON rl.map_id = m.id "
                f"WHERE {cond_mode}",
                params_mode,
            )
            mode_total = cursor.fetchone()[0]
        else:
            cond_mode = cond_rank
            params_mode = params_rank
            mode_total = rank_total

        if map_id is not None:
            cond_map = f"{cond_mode} AND rl.map_id=%s"
            params_map = params_mode + [map_id]
            cursor.execute(
                f"SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
                "JOIN _maps m ON rl.map_id = m.id "
                f"WHERE {cond_map}",
                params_map,
            )
            map_total = cursor.fetchone()[0]
        else:
            map_total = mode_total

        cursor.close()

    return {"all": overall, "rank": rank_total, "mode": mode_total, "map": map_total}

def matchup_rates(brawler_id, season_id=None, rank_id=None, mode_id=None, map_id=None):
    """指定した階層での対キャラ勝率を集計する"""
    with get_engine().connect() as conn:
        query = (
            "SELECT w.win_brawler_id, w.lose_brawler_id, COUNT(*) AS cnt "
            "FROM win_lose_logs w "
            "JOIN battle_logs bl ON w.battle_log_id = bl.id "
            "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
            "JOIN _maps m ON rl.map_id = m.id WHERE 1=1"
        )
        params: list = []
        if rank_id is not None:
            query += " AND rl.rank_id=%s"
            params.append(rank_id)
        if mode_id is not None:
            query += " AND m.mode_id=%s"
            params.append(mode_id)
        if map_id is not None:
            query += " AND rl.map_id=%s"
            params.append(map_id)
        if season_id is not None:
            start, next_start = season_range(season_id)
            query += " AND SUBSTRING(bl.id,1,8) >= %s AND SUBSTRING(bl.id,1,8) < %s"
            params.extend([start.strftime("%Y%m%d"), next_start.strftime("%Y%m%d")])
        query += " GROUP BY w.win_brawler_id, w.lose_brawler_id"
        df = pd.read_sql_query(query, conn, params=params)
    wins = (
        df[df["win_brawler_id"] == brawler_id]
        .set_index("lose_brawler_id")["cnt"]
        .rename("wins")
    )
    losses = (
        df[df["lose_brawler_id"] == brawler_id]
        .set_index("win_brawler_id")["cnt"]
        .rename("losses")
    )
    merged = pd.concat([wins, losses], axis=1).fillna(0)
    merged["total"] = merged["wins"] + merged["losses"]
    merged["win_rate"] = (merged["wins"] / merged["total"]) * 100
    with get_engine().connect() as conn:
        brawlers = pd.read_sql_query("SELECT id, name_ja FROM _brawlers", conn).set_index("id")
    merged = merged.join(brawlers).reset_index().rename(
        columns={"index": "brawler_id", "name_ja": "opponent"}
    )
    return merged[["opponent", "wins", "losses", "win_rate"]].sort_values(
        "win_rate", ascending=False
    )
def main():
    st.title("Brawl Stars 統計ダッシュボード")
    st.caption("データベースをリアルタイムで監視")
    if st_autorefresh:
        st_autorefresh(interval=60 * 1000, key="data_refresh")

    seasons = load_seasons()
    season_options = ["全体"] + [f"シーズン{s}" for s in seasons]
    season_name = st.selectbox("シーズン", season_options)
    if season_name == "全体":
        season_id = None
    else:
        season_id = int(season_name.replace("シーズン", ""))

    ranks = load_ranks()
    rank_options = ["全体"] + ranks["name_ja"].tolist()
    rank_name = st.selectbox("ランク", rank_options)
    if rank_name == "全体":
        rank_id = None
    else:
        rank_id = int(ranks[ranks["name_ja"] == rank_name]["id"].iloc[0])

    modes = load_modes()
    mode_options = ["全体"] + modes["name_ja"].tolist()
    mode_name = st.selectbox("モード", mode_options)
    if mode_name == "全体":
        mode_id = None
        maps = pd.DataFrame(columns=["id", "name_ja"])
    else:
        mode_id = int(modes[modes["name_ja"] == mode_name]["id"].iloc[0])
        maps = load_maps(mode_id)

    map_options = ["全体"] + maps["name_ja"].tolist()
    map_name = st.selectbox("マップ", map_options)
    if map_name == "全体":
        map_id = None
    else:
        map_id = int(maps[maps["name_ja"] == map_name]["id"].iloc[0])

    counts = battle_counts(
        season_id=season_id, rank_id=rank_id, mode_id=mode_id, map_id=map_id
    )
    st.caption(
        f"全体: {counts['all']} / ランク: {counts['rank']} / モード: {counts['mode']} / マップ: {counts['map']}"
    )

    st.header("キャラ使用率")
    usage_df = brawler_usage(
        season_id=season_id, rank_id=rank_id, mode_id=mode_id, map_id=map_id
    )
    if not usage_df.empty:
        st.bar_chart(usage_df.set_index("brawler")["usage_rate"])
    else:
        st.write("データがありません")

    st.header("キャラ勝率")
    win_df = brawler_win_rate(
        season_id=season_id, rank_id=rank_id, mode_id=mode_id, map_id=map_id
    )
    if not win_df.empty:
        st.bar_chart(win_df.set_index("brawler")["win_rate"])
    else:
        st.write("データがありません")

    st.header("対キャラ勝率")
    with get_engine().connect() as conn:
        brawlers = pd.read_sql_query("SELECT id, name_ja FROM _brawlers", conn)
    brawler_name = st.selectbox("キャラ", brawlers["name_ja"])
    brawler_id = int(brawlers[brawlers["name_ja"] == brawler_name]["id"].iloc[0])
    match_df = matchup_rates(
        brawler_id,
        season_id=season_id,
        rank_id=rank_id,
        mode_id=mode_id,
        map_id=map_id,
    )
    if not match_df.empty:
        st.dataframe(match_df)
    else:
        st.write("データがありません")

if __name__ == "__main__":
    main()
