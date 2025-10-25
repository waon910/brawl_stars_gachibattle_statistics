ALTER TABLE players
    ADD COLUMN is_monitored TINYINT(1) NOT NULL DEFAULT 0 AFTER last_fetched,
    ADD COLUMN monitoring_started_at DATETIME NULL DEFAULT NULL AFTER is_monitored,
    ADD INDEX idx_players_monitoring_fetch (is_monitored, last_fetched);

-- 既存データの安全のため明示的に 0 を再設定
UPDATE players SET is_monitored = 0 WHERE is_monitored IS NULL;
