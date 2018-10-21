from database import getSession
from player import Player
from replay import Replay
from sqlalchemy import func, and_
import datetime

now = datetime.datetime.now()
Past30 = now - datetime.timedelta(days=30)
Past7 = now - datetime.timedelta(days=7)
Past1 = now - datetime.timedelta(days=1)

filter_week = (Replay.start_time >= Past7, )
filter_month = (Replay.start_time >= Past30, )

player_win = ((Player.team == 0) & (Replay.radiant_win))

test = getSession()
r_week = test.query(Replay.match_id).filter(*filter_week)

f_query = test.query(Player.hero_id)
r_win = test.query(Player.hero_id, Player.team, func.count(Player.hero_id))\
            .filter(and_(Replay.radiant_win == 1, Player.is_pick == 1))\
            .group_by(Replay.match_id, Player.hero_id, Player.team)
r_lose = test.query(Player.hero_id, Player.team, func.count(Player.hero_id))\
             .filter(and_(Replay.radiant_win == 0, Player.is_pick == 1))\
             .group_by(Replay.match_id, Player.hero_id, Player.team)