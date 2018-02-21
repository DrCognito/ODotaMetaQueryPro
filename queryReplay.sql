SELECT
public_matches.duration,
public_matches.match_id,
public_matches.avg_mmr,
public_matches.avg_rank_tier,
public_matches.start_time,
public_matches.game_mode
FROM public_matches
WHERE public_matches.avg_mmr >= 6000 AND public_matches.avg_rank_tier >= 70 AND public_matches.start_time > REP_TIME
ORDER BY public_matches.match_id
LIMIT 200 OFFSET REP_OFFSET
