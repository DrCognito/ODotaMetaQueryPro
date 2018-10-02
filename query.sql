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
ORDER BY matches.match_id DESC
WHERE matches.match_id > %MINIMUM_ID% AND matches.start_time > %MINIMUM_TIME%
LIMIT 200 OFFSET REP_OFFSET
