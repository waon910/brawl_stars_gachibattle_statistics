-- 最高ランクが9以下のプレイヤーを削除し、削除した件数を出力するスクリプト
DELETE FROM players
WHERE highest_rank BETWEEN 10 AND 12;

SELECT ROW_COUNT() AS deleted_low_rank_player_count;
