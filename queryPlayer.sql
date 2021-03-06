SELECT
public_matches.match_id,
((public_player_matches.player_slot < 128) = public_matches.radiant_win) win,
public_player_matches.hero_id
FROM public_matches
JOIN public_player_matches using(match_id)
WHERE public_player_matches.match_id IN (REPLAY_LIST)
LIMIT 2000;