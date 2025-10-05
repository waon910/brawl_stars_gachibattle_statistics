-- 名前がNULLのプレイヤーを削除し、削除した件数を出力するスクリプト
DELETE FROM players
WHERE name IS NULL;

SELECT ROW_COUNT() AS deleted_unnamed_player_count;
