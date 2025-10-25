ALTER TABLE win_lose_logs
    ADD COLUMN win_player_tag VARCHAR(20) NOT NULL DEFAULT '' AFTER win_brawler_id,
    ADD COLUMN lose_player_tag VARCHAR(20) NOT NULL DEFAULT '' AFTER lose_brawler_id;
