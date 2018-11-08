SELECT 
public_player_matches.hero_id, 
COUNT(CASE WHEN ((public_player_matches.player_slot < 128) = public_matches.radiant_win) THEN 1 END) AS wins, 
COUNT(public_player_matches.hero_id) AS total 
FROM public_matches 
JOIN public_player_matches ON public_matches.match_id = public_player_matches.match_id 
WHERE public_matches.avg_mmr >= 5000 
AND public_matches.avg_rank_tier >= 72 
AND public_matches.start_time < %START_TIME% 
AND public_matches.start_time >= %ND_TIME% 
GROUP BY public_player_matches.hero_id;