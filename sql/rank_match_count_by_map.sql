-- マップごとのランクマッチ数を出力するクエリ
SELECT
    m.id AS map_id,
    m.name AS map_name,
    m.name_ja AS map_name_ja,
    mo.id AS mode_id,
    mo.name AS mode_name,
    mo.name_ja AS mode_name_ja,
    COUNT(rl.id) AS rank_match_count
FROM
    rank_logs AS rl
    INNER JOIN _maps AS m ON rl.map_id = m.id
    LEFT JOIN _modes AS mo ON m.mode_id = mo.id
GROUP BY
    m.id,
    m.name,
    m.name_ja,
    mo.id,
    mo.name,
    mo.name_ja
ORDER BY
    rank_match_count DESC,
    m.id;
