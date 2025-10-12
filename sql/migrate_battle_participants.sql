START TRANSACTION;

INSERT IGNORE INTO battle_participants (battle_log_id, team_side, brawler_id)
SELECT wl.battle_log_id, 'win' AS team_side, wl.win_brawler_id
FROM win_lose_logs wl;

INSERT IGNORE INTO battle_participants (battle_log_id, team_side, brawler_id)
SELECT wl.battle_log_id, 'lose' AS team_side, wl.lose_brawler_id
FROM win_lose_logs wl;

COMMIT;
