from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import player
import replay
import odota


engine = create_engine('sqlite:///public_data.db', echo=False)


def getSession():
    Session = sessionmaker(bind=engine)
    return Session()


def updateReplays(timeCut, session=getSession()):
    latestDBTime = replay.getLatestTime(session)

    if latestDBTime > timeCut:
        print("timeCut moved to {} from the latest replay in the database."
              .format(latestDBTime))
        timeCut = latestDBTime

    replays = odota.getReplays(timeCut)
    replay.importOdotaJSON(replays, session)


def listMissingPlayers(session=getSession()):
    query = session.query(replay.Replay.match_id)
    subquery = session.query(player.Player.match_id)
    query = query.filter(replay.Replay.match_id.notin_(subquery))

    return query


def chunker(itterable, size):
    # https://stackoverflow.com/questions/434287/what-is-the-most-pythonic-way-to-iterate-over-a-list-in-chunks
    return (itterable[pos:pos + size]
            for pos in range(0, len(itterable), size))


def updatePlayers(session=getSession()):
    # Comes back as a tuple unfortunately
    todo = listMissingPlayers(session)
    todo = [x[0] for x in todo]
    print("Retrieving players for {} matches.".format(len(todo)))

    # We get max 10 players per match, targetting 200 results per query.
    for section in chunker(todo, 200):
        playerData = odota.getPlayers(section)
        if playerData is None:
            print("Exiting due to no playerData retrieved for section:\n{}."
                  .format(section))
            break

        # Make a hashlist so we eliminate duplicates and exclude unknown 0
        # heroes
        # playerHashList = {(p['match_id'], p['hero_id']): p['win']
        #                   for p in playerData if p['hero_id'] != 0}

        player.importOdotaJSON(playerData, session)
