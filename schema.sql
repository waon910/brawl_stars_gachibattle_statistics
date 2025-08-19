-- players table
CREATE TABLE IF NOT EXISTS players (
    tag TEXT PRIMARY KEY,
    last_fetched INTEGER
);

-- battle_logs table
CREATE TABLE IF NOT EXISTS battle_logs (
    battle_time TEXT,
    star_player_tag TEXT,
    mode TEXT,
    map TEXT,
    winning_team INTEGER,
    data TEXT,
    PRIMARY KEY (battle_time, star_player_tag)
);
