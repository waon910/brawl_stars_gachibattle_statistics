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
        return pd.read_sql_query("SELECT id, name_ja FROM _maps WHERE mode_id=? ORDER BY id", conn, params=(mode_id,))

def load_ranks():
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query("SELECT id, name_ja FROM _ranks ORDER BY id", conn)

def brawler_usage(map_id, rank_id):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            """
            SELECT b.name_ja AS brawler, bur.count
            FROM brawler_used_ranks bur
            JOIN _brawlers b ON bur.brawler_id = b.id
            WHERE bur.map_id=? AND bur.rank_id=?
            """,
            conn,
            params=(map_id, rank_id),
        )
    total = df["count"].sum()
    if total > 0:
        df["usage_rate"] = df["count"] / total * 100
    else:
        df["usage_rate"] = 0
    return df.sort_values("usage_rate", ascending=False)

def brawler_win_rate(map_id, rank_id):
    with sqlite3.connect(DB_PATH) as conn:
        wins = pd.read_sql_query(
            """
            SELECT w.win_brawler_id AS brawler_id, COUNT(*) AS wins
            FROM win_lose_logs w
            JOIN battle_logs bl ON w.battle_log_id = bl.id
            JOIN rank_logs rl ON bl.rank_log_id = rl.id
            WHERE rl.map_id=? AND rl.rank_id=?
            GROUP BY w.win_brawler_id
            """,
            conn,
            params=(map_id, rank_id),
        )
        losses = pd.read_sql_query(
            """
            SELECT w.lose_brawler_id AS brawler_id, COUNT(*) AS losses
            FROM win_lose_logs w
            JOIN battle_logs bl ON w.battle_log_id = bl.id
            JOIN rank_logs rl ON bl.rank_log_id = rl.id
            WHERE rl.map_id=? AND rl.rank_id=?
            GROUP BY w.lose_brawler_id
            """,
            conn,
            params=(map_id, rank_id),
        )
    df = pd.merge(wins, losses, on="brawler_id", how="outer").fillna(0)
    df["total"] = df["wins"] + df["losses"]
    df["win_rate"] = (df["wins"] / df["total"]) * 100
    brawlers = pd.read_sql_query("SELECT id, name_ja FROM _brawlers", conn)
    df = df.merge(brawlers, left_on="brawler_id", right_on="id").drop("id", axis=1)
    return df.rename(columns={"name_ja": "brawler"}).sort_values("win_rate", ascending=False)

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
    mode_name = st.selectbox("モード", modes["name_ja"])
    mode_id = int(modes[modes["name_ja"] == mode_name]["id"].iloc[0])

    maps = load_maps(mode_id)
    map_name = st.selectbox("マップ", maps["name_ja"])
    map_id = int(maps[maps["name_ja"] == map_name]["id"].iloc[0])

    ranks = load_ranks()
    rank_name = st.selectbox("ランク", ranks["name_ja"])
    rank_id = int(ranks[ranks["name_ja"] == rank_name]["id"].iloc[0])

    st.header("キャラ使用率")
    usage_df = brawler_usage(map_id, rank_id)
    if not usage_df.empty:
        st.bar_chart(usage_df.set_index("brawler")["usage_rate"])
    else:
        st.write("データがありません")

    st.header("キャラ勝率")
    win_df = brawler_win_rate(map_id, rank_id)
    if not win_df.empty:
        st.bar_chart(win_df.set_index("brawler")["win_rate"])
    else:
        st.write("データがありません")

    st.header("対キャラ勝率")
    with sqlite3.connect(DB_PATH) as conn:
        brawlers = pd.read_sql_query("SELECT id, name_ja FROM _brawlers",conn)
    brawler_name = st.selectbox("キャラ", brawlers["name_ja"])
    brawler_id = int(brawlers[brawlers["name_ja"] == brawler_name]["id"].iloc[0])
    match_df = matchup_rates(map_id, brawler_id)
    if not match_df.empty:
        st.dataframe(match_df)
    else:
        st.write("データがありません")

if __name__ == "__main__":
    main()
