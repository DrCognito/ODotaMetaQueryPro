from math import sqrt

from pandas import DataFrame, Series
from sqlalchemy import (BigInteger, Boolean, Column, ForeignKey, Integer, and_,
                        case, create_engine, func)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import select

from heroTools import heroByID, heroShortName
#import .replay
from replay import Replay

Base = declarative_base()


class Player(Base):
    __tablename__ = "players"

    match_id = Column(BigInteger, ForeignKey(Replay.match_id),
                      primary_key=True)
    hero_id = Column(Integer, primary_key=True)
    is_pick = Column(Boolean)
    team = Column(Integer)
    order = Column(Integer)


engine = create_engine('sqlite:///public_data.db', echo=False)
# Make all our tables
Base.metadata.create_all(engine)


def getFilteredHeroResults(session, hero_id, playerFilter=None, replayFilter=None):
    '''playerFilter example: playerFilter = (hero_id = 69, match_id > 50)
       which expands as the following session.query(Player).filter(*playerFilter)

       replayFilter example: replayFilter = (duration > 2000, avg_mmr > 6500)
       which expands as the following session.query(Replays).filter(*replayFilter)
       '''

    if replayFilter is not None:
        subquery = session.query(Replay.match_id).filter(*replayFilter)
    else:
        subquery = session.query(Replay.match_id)
    #subquery = session.query(Replay.match_id).filter(Replay.start_time >= replayFilter)
    query = session.query(Player,
            ((Player.team == 0) & (Replay.radiant_win)).label('win'))\
            .filter(*playerFilter)
    query = query.filter(Player.match_id.in_(subquery))

    return buildTable(query, hero_id)


def getFilteredResults(session, replayFilter=None):

    if replayFilter is not None:
        replayFilter = and_(Player.is_pick == True, *replayFilter)
    else:
        replayFilter = Player.is_pick == True

    # Count player wins with a case expression.
    # Unfortunately radiant is team 0, and radiant wins are tracked.
    count_expression = case([(Player.team != Replay.radiant_win, 1)], else_=0)
    query = session.query(Player.hero_id,
                          func.sum(count_expression),
                          func.count(Player.hero_id))\
                   .join(Replay, Replay.match_id == Player.match_id)\
                   .filter(replayFilter)\
                   .group_by(Player.hero_id)

    output = DataFrame(columns=['Name', 'Wins', 'Losses', 'Picks',
                                'Win Rate', 'Stat. Error'])

    for h_id, wins, picks in query:
        name = heroShortName[heroByID[h_id]]
        if h_id not in output.index:
            output.loc[name] = 0
        output['Name'][name] = name
        output['Wins'][name] = wins
        output['Picks'][name] = picks

    output['Losses'] = output['Picks'] - output['Wins']

    for h in output.index:
        if output['Wins'][h] + output['Losses'][h] != 0:
            output['Win Rate'][h] = output['Wins'][h] /\
                             (output['Wins'][h] + output['Losses'][h])
            output['Stat. Error'][h] = output['Win Rate'][h] /\
                sqrt(output['Wins'][h] + output['Losses'][h])

    return output


def buildTable(query, hero_id):
    output = Series(0, index=['ID', 'Name', 'Wins', 'Losses', 'Picks',
                              'Win Rate'])
    results = query.all()

    output['ID'] = hero_id
    output['Name'] = heroShortName[heroByID[hero_id]]

    for p in results:
        output['Picks'] += 1
        if p[1]:
            output['Wins'] += 1
        else:
            output['Losses'] += 1

    if output['Wins'] + output['Losses'] != 0:
        output['Win Rate'] = output['Wins'] /\
                             (output['Wins'] + output['Losses'])
        output['Stat. Error'] = output['Win Rate'] /\
                                sqrt(output['Wins'] + output['Losses'])

    return output


def buildTableTime(query, hero_id):
    output = DataFrame(index=['Time'])
    name = heroShortName[heroByID[hero_id]]

    for p, r in query.all():
        i = Series()
        i['Time'] = r.start_time
        i['ID'] = hero_id
        i['Name'] = name

        if p[1]:
            i['Win'] = 1
            i['Loss'] = 0
        else:
            i['Win'] = 0
            i['Loss'] = 1

        output = output.append(i, ignore_index=True)

    output = output.set_index('Time')
    return output


def getHeroResults(session, hero_id):
    query = session.query(Player,
            ((Player.team == 0) & (Replay.radiant_win)).label('win'))\
            .filter(Player.hero_id == hero_id)
    return buildTable(query, hero_id)


def getHeroResultsTime(session, hero_id):
    query = session.query(Player, Replay).\
                    join(Replay).\
                    filter(Player.hero_id == hero_id)

    return buildTableTime(query, hero_id)


def importOdotaJSON(match_id, data, session):
    try:
        for r in data:
            if r['hero_id'] == 0:
                print("Skipped invalid hero in {}".format(r['match_id']))
                continue
            newPlayer = Player(**r, match_id=match_id)
            session.add(newPlayer)
    except Exception:
        session.rollback()
        raise
    session.commit()
