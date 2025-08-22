import sqlite3
import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    st_autorefresh = None

DB_PATH = "brawl_stats.db"

# @st.cache_resource
# def get_connection():
#     return sqlite3.connect(DB_PATH)

def load_modes():
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query("SELECT id, name_ja FROM _modes ORDER BY id", conn)

def load_maps(mode_id):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT id, name_ja FROM _maps WHERE mode_id=? ORDER BY id",
            conn,
            params=(mode_id,),
        )

def load_ranks():
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query("SELECT id, name_ja FROM _ranks ORDER BY id", conn)

def brawler_usage(mode_id, map_id=None, rank_id=None):
    """指定した階層でのキャラ使用率を集計する"""
    query = (
        "SELECT b.name_ja AS brawler, SUM(bur.count) AS count "
        "FROM brawler_used_ranks bur "
        "JOIN _brawlers b ON bur.brawler_id = b.id "
        "JOIN _maps m ON bur.map_id = m.id WHERE 1=1"
    )
    params = []
    if mode_id is not None:
        query += " AND m.mode_id=?"
        params.append(mode_id)
    if map_id is not None:
        query += " AND bur.map_id=?"
        params.append(map_id)
    if rank_id is not None:
        query += " AND bur.rank_id=?"
        params.append(rank_id)
    query += " GROUP BY b.name_ja"
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=params)
    total = df["count"].sum()
    if total > 0:
        df["usage_rate"] = df["count"] / total * 100
    else:
        df["usage_rate"] = 0
    return df.sort_values("usage_rate", ascending=False)

def brawler_win_rate(mode_id, map_id=None, rank_id=None):
    """指定した階層でのキャラ勝率を集計する"""
    base = (
        "FROM win_lose_logs w "
        "JOIN battle_logs bl ON w.battle_log_id = bl.id "
        "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
        "JOIN _maps m ON rl.map_id = m.id WHERE 1=1"
    )
    params = []
    if mode_id is not None:
        base += " AND m.mode_id=?"
        params.append(mode_id)
    if map_id is not None:
        base += " AND rl.map_id=?"
        params.append(map_id)
    if rank_id is not None:
        base += " AND rl.rank_id=?"
        params.append(rank_id)
    with sqlite3.connect(DB_PATH) as conn:
        wins = pd.read_sql_query(
            "SELECT w.win_brawler_id AS brawler_id, COUNT(*) AS wins " + base + " GROUP BY w.win_brawler_id",
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

def battle_counts(mode_id, map_id=None, rank_id=None):
    """各階層での対戦数を取得する"""
    with sqlite3.connect(DB_PATH) as conn:
        overall = conn.execute("SELECT COUNT(*) FROM battle_logs").fetchone()[0]
        if mode_id is not None:
            mode = conn.execute(
                "SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
                "JOIN _maps m ON rl.map_id = m.id WHERE m.mode_id=?",
                (mode_id,),
            ).fetchone()[0]
        else:
            mode = overall
        if map_id is not None:
            map_total = conn.execute(
                "SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id WHERE rl.map_id=?",
                (map_id,),
            ).fetchone()[0]
        else:
            map_total = mode
        if map_id is not None and rank_id is not None:
            rank_total = conn.execute(
                "SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id "
                "WHERE rl.map_id=? AND rl.rank_id=?",
                (map_id, rank_id),
            ).fetchone()[0]
        elif map_id is None and rank_id is not None:
            rank_total = conn.execute(
                "SELECT COUNT(*) FROM battle_logs bl "
                "JOIN rank_logs rl ON bl.rank_log_id = rl.id WHERE rl.rank_id=?",
                (rank_id,),
            ).fetchone()[0]
        else:
            rank_total = map_total
    return {"all": overall, "mode": mode, "map": map_total, "rank": rank_total}

def matchup_rates(map_id, brawler_id):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            """
            SELECT w.win_brawler_id, w.lose_brawler_id, COUNT(*) AS cnt
            FROM win_lose_logs w
            JOIN battle_logs bl ON w.battle_log_id = bl.id
            JOIN rank_logs rl ON bl.rank_log_id = rl.id
            WHERE rl.map_id=?
            GROUP BY w.win_brawler_id, w.lose_brawler_id
            """,
            conn,
            params=(map_id,),
        )
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
    with sqlite3.connect(DB_PATH) as conn:
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

    # conn = get_connection()
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

    ranks = load_ranks()
    rank_options = ["全体"] + ranks["name_ja"].tolist()
    rank_name = st.selectbox("ランク", rank_options)
    if rank_name == "全体":
        rank_id = None
    else:
        rank_id = int(ranks[ranks["name_ja"] == rank_name]["id"].iloc[0])

    counts = battle_counts(mode_id, map_id, rank_id)
    st.caption(
        f"全体: {counts['all']} / モード: {counts['mode']} / マップ: {counts['map']} / ランク: {counts['rank']}"
    )

    st.header("キャラ使用率")
    usage_df = brawler_usage(mode_id, map_id, rank_id)
    if not usage_df.empty:
        st.bar_chart(usage_df.set_index("brawler")["usage_rate"])
    else:
        st.write("データがありません")

    st.header("キャラ勝率")
    win_df = brawler_win_rate(mode_id, map_id, rank_id)
    if not win_df.empty:
        st.bar_chart(win_df.set_index("brawler")["win_rate"])
    else:
        st.write("データがありません")

    st.header("対キャラ勝率")
    with sqlite3.connect(DB_PATH) as conn:
        brawlers = pd.read_sql_query("SELECT id, name_ja FROM _brawlers", conn)
    brawler_name = st.selectbox("キャラ", brawlers["name_ja"])
    brawler_id = int(brawlers[brawlers["name_ja"] == brawler_name]["id"].iloc[0])
    if map_id is not None:
        match_df = matchup_rates(map_id, brawler_id)
        if not match_df.empty:
            st.dataframe(match_df)
        else:
            st.write("データがありません")
    else:
        st.write("マップを選択すると対キャラ勝率を表示します")

if __name__ == "__main__":
    main()
