from sqlalchemy import Column, Integer, BigInteger, Boolean, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from pandas import Series
import replay
from heroTools import heroByID, heroShortName
from math import sqrt

Base = declarative_base()


class Player(Base):
    __tablename__ = "players"

    match_id = Column(BigInteger, ForeignKey(replay.Replay.match_id),
                      primary_key=True)
    hero_id = Column(Integer, primary_key=True)
    win = Column(Boolean)


engine = create_engine('sqlite:///public_data.db', echo=False)
# Make all our tables
Base.metadata.create_all(engine)


def getFilteredHeroResults(session, hero_id, playerFilter=None, replayFilter=None):
    '''playerFilter example: playerFilter = (hero_id = 69, match_id > 50)
       which expands as the following session.query(Player).filter(*playerFilter)

       replayFilter example: replayFilter = (duration > 2000, avg_mmr > 6500)
       which expands as the following session.query(Replays).filter(*replayFilter)
       '''

    subquery = session.query(replay.Replay.match_id).filter(*replayFilter)
    #subquery = session.query(replay.Replay.match_id).filter(replay.Replay.start_time >= replayFilter)
    query = session.query(Player).filter(*playerFilter)
    query = query.filter(Player.match_id.in_(subquery))

    return buildTable(query, hero_id)


def buildTable(query, hero_id):
    output = Series(0, index=['ID', 'Name', 'Wins', 'Losses', 'Picks',
                              'Win Rate'])
    results = query.all()

    output['ID'] = hero_id
    output['Name'] = heroShortName[heroByID[hero_id]]

    for p in results:
        output['Picks'] += 1
        if p.win:
            output['Wins'] += 1
        else:
            output['Losses'] += 1

    if output['Wins'] + output['Losses'] != 0:
        output['Win Rate'] = output['Wins'] /\
                             (output['Wins'] + output['Losses'])
        output['Stat. Error'] = output['Win Rate'] /\
                                sqrt(output['Wins'] + output['Losses'])

    return output


def getHeroResults(session, hero_id):
    query = session.query(Player).filter(Player.hero_id == hero_id)
    return buildTable(query, hero_id)


def importOdotaJSON(data, session):
    try:
        for r in data:
            if r['hero_id'] == 0:
                print("Skipped invalid hero in {}".format(r['match_id']))
                continue
            newPlayer = Player(**r)
            #newPlayer = Player(match_id=r[0], hero_id=r[1], win=data[r])
            session.add(newPlayer)
            #session.commit()
    except Exception:
        session.rollback()
        raise
    session.commit()
