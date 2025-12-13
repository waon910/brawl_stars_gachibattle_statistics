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

-- 修正後のクエリ
SELECT
    p.current_rank,
    COUNT(p.tag) AS player_count
FROM
    players AS p
GROUP BY
    p.current_rank
ORDER BY
    p.current_rank;

SELECT
    *
FROM
    players
WHERE
    current_rank = 22;

