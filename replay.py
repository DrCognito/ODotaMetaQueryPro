from sqlalchemy import Column, Integer, BigInteger, DateTime, Float
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound
import datetime

Base = declarative_base()
Patch_7_07 = datetime.datetime(2017, 11, 2, 0, 0, 0, 0)
defaultTime = Patch_7_07


class Replay(Base):
    __tablename__ = "replays"

    match_id = Column(BigInteger, primary_key=True)
    start_time = Column(DateTime)
    duration = Column(Integer)
    avg_mmr = Column(Integer)
    avg_rank_tier = Column(Float)
    game_mode = Column(Integer)


engine = create_engine('sqlite:///public_data.db', echo=False)
# Make all our tables
Base.metadata.create_all(engine)


def getLatestTime(session):
    try:
        result = session.query(Replay).order_by(Replay.start_time.desc())\
                .first()
    except NoResultFound:
        print("Warning! No results found in database!")
        return defaultTime
    if result is None:
        return defaultTime
    return result.start_time


def getLatestReplayID(session):
    result = session.query(Replay).order_by(Replay.match_id.desc()).first()
    if not result:
        print("Warning no replays present.")
        return 0

    return result.match_id


def importOdotaJSON(data, session):
    try:
        for r in data:
            r['start_time'] = datetime.datetime.fromtimestamp(r['start_time'])
            newReplay = Replay(**r)
            session.add(newReplay)
    except Exception:
        session.rollback()
        raise
    session.commit()
