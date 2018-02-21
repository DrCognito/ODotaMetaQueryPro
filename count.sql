SELECT
COUNT (*)
FROM
public_matches
WHERE public_matches.avg_mmr >= 6000 
AND public_matches.avg_rank_tier >= 70 
AND public_matches.start_time > REP_TIME
AND public_matches.avg_rank_tier IS NOT NULL