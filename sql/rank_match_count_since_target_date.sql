-- 対象期間（対象日付から現在）でのランクマッチ件数を出力するクエリ
-- 対象日付は YYYYMMDD 形式で ? プレースホルダに指定してください。
SELECT
    COUNT(*) AS rank_match_count
FROM
    rank_logs AS rl
WHERE
    rl.id >= "20250805";
