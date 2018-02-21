import requests
import time
from replay import Patch_7_07

url = "https://api.opendota.com/api/explorer"
# sql = open('queryMin.sql', 'r').read()

# params = {
#     'sql': sql
# }
# print(params)
# r = requests.get(url, params=params)
# print(r.json())


def getReplays(timeCut=Patch_7_07, offSet=0):
    print("Getting replays {} to {} (max)".format(offSet, offSet+200))
    timeStr = timeCut.timestamp()

    with open('queryReplay.sql', 'r') as sql:
        query = sql.read()
        query = query.replace("REP_TIME", str(timeStr))
        query = query.replace("REP_OFFSET", str(offSet))

        rows = []
        r = requests.get(url, params={'sql': query})
        time.sleep(1)
        if r.status_code != requests.codes.ok:
            print("Bad status code for {} returned {}".format(r.url,
                                                              r.status_code))
            print("Query was:\n{}".format(query))
            return
        data = r.json()
        # We grab 200 at once so if we get 200 presumably theres more.
        if data['rowCount'] == 200:
            rows += data['rows']
            rows += getReplays(timeCut, offSet + 200)
        else:
            rows += data['rows']

        return rows


def getPlayers(matchList):
    print("Getting players for matches {}".format(matchList))

    with open('queryPlayer.sql', 'r') as sql:
        query = sql.read()
        query = query.replace("REPLAY_LIST", str(matchList).strip('[]'))
        r = requests.get(url, params={'sql': query})

    if r.status_code != requests.codes.ok:
        print("Bad status code for {} returned {}".format(r.url,
                                                          r.status_code))
        print("Query was:\n{}".format(query))
        return None
    time.sleep(1)

    data = r.json()
    return data['rows']


def getCount(timeCut=Patch_7_07):
    timeStr = str(timeCut.timestamp())

    with open('count.sql', 'r') as sql:
        query = sql.read()
        query = query.replace("REP_TIME", timeStr)

        r = requests.get(url, params={'sql': query})

        if r.status_code != requests.codes.ok:
            print("Bad status code for {} returned {}".format(r.url,
                                                              r.status_code))
            print("Query was:\n{}".format(query))
            return
        data = r.json()
        return data['rows'][0]['count']