-- 最高ランク・現在ランクともに15以下で、監視対象ではないプレイヤーを削除
DELETE FROM players
WHERE is_monitored = 0
   AND (current_rank <= 15
   AND highest_rank <= 16);
