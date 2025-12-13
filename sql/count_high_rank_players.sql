-- 現在ランクが18以上、または最高ランクが21以上のプレイヤー数をカウント
SELECT COUNT(*) AS high_rank_player_count
FROM players
WHERE current_rank >= 18
   OR highest_rank >= 21;

SELECT COUNT(*) AS high_rank_player_count
FROM players
WHERE is_monitored = 0
   AND (current_rank <= 15
   AND highest_rank <= 16);

SELECT *
FROM players
WHERE is_monitored = 0
   AND (current_rank <= 12
   OR highest_rank <= 12);