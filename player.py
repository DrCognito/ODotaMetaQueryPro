from sqlalchemy import Column, Integer, BigInteger, Boolean, ForeignKey, create_engine
from sqlalchemy import and_, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import select
from pandas import Series, DataFrame
#import .replay
from replay import Replay
from heroTools import heroByID, heroShortName
from math import sqrt

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
        replayFilter_win = and_(Replay.radiant_win == 1, *replayFilter)
        replayFilter_loss = and_(Replay.radiant_win == 0, *replayFilter)
    else:
        replayFilter_win = (Replay.radiant_win == 1)
        replayFilter_loss = (Replay.radiant_win == 0)

    subquery_win = session.query(Replay.match_id)\
                          .filter(replayFilter_win)
    subquery_lose = session.query(Replay.match_id)\
                           .filter(replayFilter_loss)

    output = DataFrame(columns=['Name', 'Wins', 'Losses', 'Picks',
                                'Win Rate'])

    r_win = session.query(Player.hero_id, Player.team, func.count(Player.hero_id))\
            .join(subquery_win)\
            .filter(and_(Replay.radiant_win == 1, Player.is_pick == 1))\
            .group_by(Replay.match_id, Player.hero_id, Player.team)
    r_lose = session.query(Player.hero_id, Player.team, func.count(Player.hero_id))\
             .join(subquery_lose)\
             .filter(and_(Replay.radiant_win == 1, Player.is_pick == 1))\
             .group_by(Replay.match_id, Player.hero_id, Player.team)

    def _process_rows(query, radiant_win):
        for h_id, team, count in query:
            name = heroShortName[heroByID[h_id]]
            if name not in output.index:
                output.loc[name] = 0
            if team == 0 and radiant_win:
                output['Wins'][name] += count
            else:
                output['Losses'][name] += count

    _process_rows(r_win, radiant_win=True)
    _process_rows(r_lose, radiant_win=False)

    for h in output:
        if output['Wins'] + output['Losses'] != 0:
            output['Win Rate'] = output['Wins'] /\
                             (output['Wins'] + output['Losses'])
            output['Stat. Error'] = output['Win Rate'] /\
                sqrt(output['Wins'] + output['Losses'])

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
