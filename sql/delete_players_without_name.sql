-- 名前がNULLのプレイヤーを削除し、削除した件数を出力するスクリプト
-- 監視対象プレイヤー (is_monitored = 1) は削除しない
DELETE FROM players
WHERE name IS NULL
  AND is_monitored = 0;

SELECT ROW_COUNT() AS deleted_unnamed_player_count;
