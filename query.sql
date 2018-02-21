SELECT
public_matches.duration,
public_matches.match_id,
public_matches.radiant_win,
public_matches.avg_mmr,
public_matches.avg_rank_tier,
((public_player_matches.player_slot < 128) = public_matches.radiant_win) win,
public_player_matches.player_slot,
public_player_matches.hero_id
FROM public_matches
JOIN public_player_matches using(match_id)
WHERE public_matches.avg_mmr > 5000
LIMIT 20
