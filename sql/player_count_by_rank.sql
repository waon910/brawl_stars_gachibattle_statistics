-- ランク別のプレイヤー数を出力するクエリ
SELECT
    p.highest_rank,
    COUNT(p.tag) AS player_count
FROM
    players AS p
GROUP BY
    p.highest_rank
ORDER BY
    p.highest_rank
