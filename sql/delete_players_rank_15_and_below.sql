-- 最高ランク・現在ランクともに15以下で、監視対象ではないプレイヤーを削除
DELETE FROM players
WHERE highest_rank <= 15
  AND current_rank <= 15
  AND is_monitored = 0;

-- 削除件数を確認
SELECT ROW_COUNT() AS deleted_low_rank_players;
