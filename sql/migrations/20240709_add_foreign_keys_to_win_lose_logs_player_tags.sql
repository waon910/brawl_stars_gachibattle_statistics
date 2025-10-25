ALTER TABLE win_lose_logs
    MODIFY COLUMN win_player_tag VARCHAR(20) NOT NULL,
    MODIFY COLUMN lose_player_tag VARCHAR(20) NOT NULL,
    ADD CONSTRAINT fk_win_lose_logs_win_player_tag FOREIGN KEY (win_player_tag) REFERENCES players(tag),
    ADD CONSTRAINT fk_win_lose_logs_lose_player_tag FOREIGN KEY (lose_player_tag) REFERENCES players(tag);
