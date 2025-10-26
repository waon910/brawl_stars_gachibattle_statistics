-- 最高ランクが9以下のプレイヤーを削除し、削除した件数を出力するスクリプト
-- 監視対象プレイヤー (is_monitored = 1) は削除しない
DELETE FROM players
WHERE highest_rank BETWEEN 0 AND 6
  AND is_monitored = 0;

SELECT ROW_COUNT() AS deleted_low_rank_player_count;
