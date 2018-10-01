SELECT
matches.match_id,
matches.radiant_win,
matches.start_time,
matches.game_mode,
matches.dire_team_id,
matches.radiant_team_id,
matches.leagueid,
matches.version,
matches.picks_bans
FROM matches
WHERE matches.match_id = 4144074021
LIMIT 1
