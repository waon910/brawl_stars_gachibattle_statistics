-- ランク別のランクマッチ数とバトル数を出力するクエリ
SELECT
    r.id AS rank_id,
    r.name AS rank_name,
    r.name_ja AS rank_name_ja,
    COUNT(DISTINCT rl.id) AS rank_match_count,
    COUNT(bl.id) AS battle_log_count
FROM
    rank_logs AS rl
    INNER JOIN _ranks AS r ON rl.rank_id = r.id
    LEFT JOIN battle_logs AS bl ON bl.rank_log_id = rl.id
GROUP BY
    r.id,
    r.name,
    r.name_ja
ORDER BY
    r.id;
