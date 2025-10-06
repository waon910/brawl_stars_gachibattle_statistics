-- ランク別のプレイヤー数を出力するクエリ
SELECT
    r.id AS rank_id,
    r.name AS rank_name,
    r.name_ja AS rank_name_ja,
    COUNT(p.tag) AS player_count
FROM
    _ranks AS r
    LEFT JOIN players AS p ON p.highest_rank = r.id
GROUP BY
    r.id,
    r.name,
    r.name_ja
ORDER BY
    r.id;
