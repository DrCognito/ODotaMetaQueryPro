from pandas import DataFrame, Series
from sqlalchemy import (BigInteger, Boolean, Column, DateTime, ForeignKey,
                        Integer, String, and_, case, create_engine, func)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import select

from heroTools import heroByID, heroShortName

Base = declarative_base()


class AmateurHero(Base):
    __tablename__ = "amateur_heroes"

    hero_id = Column(Integer, primary_key=True)
    name = Column(String)
    start_time = Column(DateTime, primary_key=True)
    end_time = Column(DateTime, primary_key=True)
    wins = Column(Integer)
    total = Column(Integer)


engine = create_engine('sqlite:///public_data.db', echo=False)
# Make all our tables
Base.metadata.create_all(engine)


def import_odota_json(start: DateTime, end: DateTime, data, session):
    try:
        for row in data:
            if row['hero_id'] == 0:
                    print("Skipped invalid hero.")
                    continue
            name = heroShortName[heroByID[row['hero_id']]]
            new_hero = AmateurHero(**row,
                                   name=name,
                                   start_time=start,
                                   end_time=end)
            session.add(new_hero)
    except SQLAlchemyError:
        session.rollback()
        raise
    else:
        session.commit()


def get_latest_times(session):
    '''Returns the earliest start_time and its corresponding end_time in
       the table of amateur hero results'''
    return session.query(AmateurHero.start_time, AmateurHero.end_time)\
                  .order_by(AmateurHero.start_time.desc()).first()


def time_exists(session, time, start_time=True):
    if start_time:
        time_to_test = AmateurHero.start_time
    else:
        time_to_test = AmateurHero.end_time
    test = session.query(AmateurHero).filter(time_to_test == time).count()

    if test == 0:
        return False
    else:
        return True


def get_results_query(session, start: DateTime, end: DateTime):
    query = session.query(AmateurHero.name, func.sum(AmateurHero.wins).label("wins"),
                          func.sum(AmateurHero.total).label("picks"))\
                   .filter(AmateurHero.start_time < start)\
                   .filter(AmateurHero.end_time >= end)\
                   .group_by(AmateurHero.hero_id)
    return query
