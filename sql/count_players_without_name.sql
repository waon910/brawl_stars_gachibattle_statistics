-- 名前がNULLのプレイヤー数を出力するクエリ
-- SELECT
--     COUNT(*) AS unnamed_player_count
-- FROM
--     players
-- WHERE
--     name IS NULL;

SELECT
    *
FROM
    players
WHERE
    name IS NULL;
