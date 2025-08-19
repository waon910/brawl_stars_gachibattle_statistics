-- players table
CREATE TABLE IF NOT EXISTS players (
    tag TEXT PRIMARY KEY,
    last_fetched INTEGER
);

-- game modes
CREATE TABLE IF NOT EXISTS modes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
    name_ja TEXT UNIQUE
);

-- maps linked to modes
CREATE TABLE IF NOT EXISTS maps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE,
    name_ja TEXT UNIQUE,
    mode_id INTEGER,
    UNIQUE(name, mode_id),
    FOREIGN KEY (mode_id) REFERENCES modes(id)
);

-- brawlers (playable characters)
CREATE TABLE IF NOT EXISTS brawlers (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);

-- battle_logs table
CREATE TABLE IF NOT EXISTS battle_logs (
    battle_time TEXT,
    star_player_tag TEXT,
    mode_id INTEGER,
    map_id INTEGER,
    rank INTEGER,
    data TEXT,
    PRIMARY KEY (battle_time, star_player_tag),
    FOREIGN KEY (mode_id) REFERENCES modes(id),
    FOREIGN KEY (map_id) REFERENCES maps(id),
);
