-- 各テーブルの件数を出力するクエリ
SELECT
    'players' AS table_name,
    COUNT(*) AS row_count
FROM
    players
UNION ALL
SELECT
    '_modes' AS table_name,
    COUNT(*) AS row_count
FROM
    _modes
UNION ALL
SELECT
    '_maps' AS table_name,
    COUNT(*) AS row_count
FROM
    _maps
UNION ALL
SELECT
    '_ranks' AS table_name,
    COUNT(*) AS row_count
FROM
    _ranks
UNION ALL
SELECT
    '_brawlers' AS table_name,
    COUNT(*) AS row_count
FROM
    _brawlers
UNION ALL
SELECT
    'rank_logs' AS table_name,
    COUNT(*) AS row_count
FROM
    rank_logs
UNION ALL
SELECT
    'rank_star_logs' AS table_name,
    COUNT(*) AS row_count
FROM
    rank_star_logs
UNION ALL
SELECT
    'battle_logs' AS table_name,
    COUNT(*) AS row_count
FROM
    battle_logs
UNION ALL
SELECT
    'win_lose_logs' AS table_name,
    COUNT(*) AS row_count
FROM
    win_lose_logs
ORDER BY
    table_name;
