CREATE TABLE IF NOT EXISTS players (
    tag VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255),
    highest_rank INT NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS rank_logs (
    id VARCHAR(50) PRIMARY KEY,
    map_id INT NOT NULL,
    rank_id INT NOT NULL,
    FOREIGN KEY (map_id) REFERENCES _maps(id),
    FOREIGN KEY (rank_id) REFERENCES _ranks(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS rank_star_logs (
    rank_log_id VARCHAR(50) PRIMARY KEY,
    star_brawler_id INT NOT NULL,
    FOREIGN KEY (rank_log_id) REFERENCES rank_logs(id) ON DELETE CASCADE,
    FOREIGN KEY (star_brawler_id) REFERENCES _brawlers(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS battle_logs (
    id VARCHAR(100) PRIMARY KEY,
    rank_log_id VARCHAR(50) NOT NULL,
    FOREIGN KEY (rank_log_id) REFERENCES rank_logs(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS battle_participants (
    battle_log_id VARCHAR(100) NOT NULL,
    team_side ENUM('win', 'lose') NOT NULL,
    brawler_id INT NOT NULL,
    PRIMARY KEY (battle_log_id, team_side, brawler_id),
    FOREIGN KEY (battle_log_id) REFERENCES battle_logs(id) ON DELETE CASCADE,
    FOREIGN KEY (brawler_id) REFERENCES _brawlers(id)
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
CREATE INDEX idx_battle_logs_rank_log_id_id ON battle_logs(rank_log_id, id);
CREATE INDEX idx_rank_logs_rank_map ON rank_logs(rank_id, map_id);
CREATE INDEX idx_rank_logs_rank_id_id ON rank_logs(rank_id, id);
CREATE INDEX idx_rank_star_logs_brawler ON rank_star_logs(star_brawler_id);
CREATE INDEX idx_win_lose_logs_battle_log_id ON win_lose_logs(battle_log_id);
CREATE INDEX idx_battle_participants_side ON battle_participants(team_side, brawler_id, battle_log_id);
CREATE INDEX idx_battle_participants_battle ON battle_participants(battle_log_id, team_side);

