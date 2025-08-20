-- players table
CREATE TABLE IF NOT EXISTS players (
    tag TEXT PRIMARY KEY,
    last_fetched DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- game modes table
CREATE TABLE IF NOT EXISTS _modes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE,
    name_ja TEXT UNIQUE
);

-- maps linked to modes
CREATE TABLE IF NOT EXISTS _maps (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    name_ja TEXT UNIQUE,
    mode_id INTEGER,
    FOREIGN KEY (mode_id) REFERENCES modes(id)
);

-- ranks table
CREATE TABLE IF NOT EXISTS _ranks (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    name_ja TEXT UNIQUE
);

-- brawlers table
CREATE TABLE IF NOT EXISTS _brawlers (
    id INTEGER PRIMARY KEY,
    name_ja TEXT UNIQUE
);

-- brawler_used_rank_logs table
CREATE TABLE IF NOT EXISTS brawler_used_ranks (
    brawler_id INTEGER NOT NULL,
    map_id INTEGER NOT NULL,
    rank_id INTEGER NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (brawler_id, map_id, rank_id),
    FOREIGN KEY (brawler_id) REFERENCES brawlers(id),
    FOREIGN KEY (map_id) REFERENCES maps(id),
    FOREIGN KEY (rank_id) REFERENCES ranks(id)
);

-- rank_logs table
CREATE TABLE IF NOT EXISTS rank_logs (
    id TEXT PRIMARY KEY,
    map_id INTEGER NOT NULL,
    rank_id INTEGER NOT NULL,
    FOREIGN KEY (map_id) REFERENCES maps(id),
    FOREIGN KEY (rank_id) REFERENCES ranks(id)
);

-- battle_logs table
CREATE TABLE IF NOT EXISTS battle_logs (
    id TEXT PRIMARY KEY, -- battleTime+starPlayer.tag
    rank_log_id INTEGER NOT NULL,
    FOREIGN KEY (rank_log_id) REFERENCES rank_logs(id)
);

-- win_lose_logs table
CREATE TABLE IF NOT EXISTS win_lose_logs (
    win_brawler_id INTEGER NOT NULL,
    lose_brawler_id INTEGER NOT NULL,
    battle_log_id TEXT NOT NULL,
    PRIMARY KEY (win_brawler_id, lose_brawler_id, battle_log_id),
    FOREIGN KEY (win_brawler_id) REFERENCES brawlers(id),
    FOREIGN KEY (lose_brawler_id) REFERENCES brawlers(id),
    FOREIGN KEY (battle_log_id) REFERENCES battle_logs(id)
);