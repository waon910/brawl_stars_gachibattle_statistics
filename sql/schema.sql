CREATE TABLE IF NOT EXISTS players (
    tag VARCHAR(20) PRIMARY KEY,
    last_fetched DATETIME NOT NULL DEFAULT '2000-01-01 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS _modes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    name_ja VARCHAR(255) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS _maps (
    id INT PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    name_ja VARCHAR(255) UNIQUE,
    mode_id INT,
    FOREIGN KEY (mode_id) REFERENCES _modes(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS _ranks (
    id INT PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    name_ja VARCHAR(255) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS _brawlers (
    id INT PRIMARY KEY,
    name_ja VARCHAR(255) UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS brawler_used_ranks (
    brawler_id INT NOT NULL,
    map_id INT NOT NULL,
    rank_id INT NOT NULL,
    count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (brawler_id, map_id, rank_id),
    FOREIGN KEY (brawler_id) REFERENCES _brawlers(id),
    FOREIGN KEY (map_id) REFERENCES _maps(id),
    FOREIGN KEY (rank_id) REFERENCES _ranks(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS rank_logs (
    id VARCHAR(50) PRIMARY KEY,
    map_id INT NOT NULL,
    rank_id INT NOT NULL,
    FOREIGN KEY (map_id) REFERENCES _maps(id),
    FOREIGN KEY (rank_id) REFERENCES _ranks(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS battle_logs (
    id VARCHAR(100) PRIMARY KEY,
    rank_log_id VARCHAR(50) NOT NULL,
    FOREIGN KEY (rank_log_id) REFERENCES rank_logs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS win_lose_logs (
    win_brawler_id INT NOT NULL,
    lose_brawler_id INT NOT NULL,
    battle_log_id VARCHAR(100) NOT NULL,
    PRIMARY KEY (win_brawler_id, lose_brawler_id, battle_log_id),
    FOREIGN KEY (win_brawler_id) REFERENCES _brawlers(id),
    FOREIGN KEY (lose_brawler_id) REFERENCES _brawlers(id),
    FOREIGN KEY (battle_log_id) REFERENCES battle_logs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_battle_logs_rank_log_id ON battle_logs(rank_log_id);
CREATE INDEX idx_rank_logs_rank_map ON rank_logs(rank_id, map_id);
CREATE INDEX idx_win_lose_logs_battle_log_id ON win_lose_logs(battle_log_id);

