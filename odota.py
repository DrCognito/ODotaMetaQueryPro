import requests
import time
from datetime import datetime, timedelta
from replay import Patch_7_07

url = "https://api.opendota.com/api/explorer"
# sql = open('queryMin.sql', 'r').read()

# params = {
#     'sql': sql
# }
# print(params)
# r = requests.get(url, params=params)
# print(r.json())


def getReplays(timeCut=Patch_7_07, latest_id=0, offSet=0):
    print("Getting replays {} to {} (max)".format(offSet, offSet+200))
    timeStr = timeCut.timestamp()

    with open('query.sql', 'r') as sql:
        query = sql.read()
        query = query.replace("%MINIMUM_TIME%", str(timeStr))
        query = query.replace("%MINIMUM_ID%", str(latest_id))
        query = query.replace("REP_OFFSET", str(offSet))

        rows = []
        time.sleep(1)
        try:
            r = requests.get(url, params={'sql': query})
        except requests.exceptions.RequestException as e:
            print("Exception raised:\n", e)
            print("Input url:", url)
            print("Input query:", query)
            return

        requestOK = r.status_code == requests.codes.ok
        if not requestOK:
            print("Bad status code for {} returned {}".format(r.url,
                                                              r.status_code))
            print("Query was:\n{}".format(query))
            return
        data = r.json()
        # We grab 200 at once so if we get 200 presumably theres more.
        if requestOK and data['rowCount'] == 200:
            rows += data['rows']
            extra = getReplays(timeCut, latest_id, offSet + 200)
            # Guard against bad requests returning none
            if extra:
                rows += extra
        elif requestOK:
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
    past_month = str((datetime.today() - timedelta(days=30)).timestamp())

    with open('count.sql', 'r') as sql:
        query = sql.read()
        query = query.replace("%MINIMUM_TIME%", past_month)

        r = requests.get(url, params={'sql': query})

        if r.status_code != requests.codes.ok:
            print("Bad status code for {} returned {}".format(r.url,
                                                              r.status_code))
            print("Query was:\n{}".format(query))
            return
        data = r.json()
        return data['rows'][0]['count']


if __name__ == "__main__":
    print(getCount())