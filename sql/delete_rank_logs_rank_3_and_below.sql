-- ランク3以下のランクマッチログと関連ログを削除するスクリプト
-- 1. 勝敗ログを削除
DELETE FROM win_lose_logs
WHERE battle_log_id IN (
    SELECT bl.id
    FROM battle_logs AS bl
    INNER JOIN rank_logs AS rl ON bl.rank_log_id = rl.id
    WHERE rl.rank_id <= 3
);

-- 2. バトルログを削除
DELETE FROM battle_logs
WHERE rank_log_id IN (
    SELECT rl.id
    FROM rank_logs AS rl
    WHERE rl.rank_id <= 3
);

-- 3. ランクマッチログを削除（rank_star_logsはON DELETE CASCADE）
DELETE FROM rank_logs
WHERE rank_id <= 3;

-- 4. 削除件数を出力
SELECT ROW_COUNT() AS deleted_rank_log_count;
