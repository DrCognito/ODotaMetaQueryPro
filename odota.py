import requests
import time
from datetime import datetime, timedelta
from replay import Patch_7_07
from amateur_player import get_latest_times, time_exists, import_odota_json

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
        time.sleep(2)
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


def update_amateur_heroes(session, n_days=30, retries=3, reacquire=False):
    '''Update the winrates per day for each hero in amateur matches.
       n_days is the maximum number of days to go back.
       if reacquire is True existing records will be requeried and replaced.
    '''
    start_day = datetime.now()
    start_day = datetime(start_day.year, start_day.month, start_day.day)
    # start, end = get_latest_times(session)

    # if start == start_day and end < (start_day - timedelta(days=n_days)):
    #     if not reacquire:
    #         print("No days to acquire for n_days {}".format(n_days))
    #         return None

    max_retries = 10
    failed_attempts = 0
    for day in range(n_days):
        rows = []
        individual_attempts = 0
        start_time = start_day - timedelta(days=day)
        end_time = start_time - timedelta(days=1)

        if time_exists(session, start_time) and not reacquire:
            print("Entries exist for {} to {}".format(start_time, end_time))
            continue
        else:
            print("Processing from {} to {}".format(start_time, end_time))

        with open('queryAmateurHeroes.sql', 'r') as sql:
            query = sql.read()
            query = query.replace("%START_TIME%",
                                  str(start_time.timestamp()))
            query = query.replace("%ND_TIME%",
                                  str(end_time.timestamp()))

            finished = False
            while(not finished):
                if failed_attempts > max_retries or individual_attempts > 3:
                    print("Failed to retrieve day {}, from {} to {}"
                          .format(day, start_time, end_time))
                    break
                try:
                    r = requests.get(url, params={'sql': query})
                    r.raise_for_status()
                except requests.exceptions.RequestException as e:
                    print("Exception raised:\n", e)
                    print("Input url:", url)
                    failed_attempts += 1
                    individual_attempts += 1
                else:
                    data = r.json()
                    rows += data['rows']
                    finished = True

                time.sleep(3)

        if rows is not None:
            import_odota_json(start_time, end_time, rows, session)


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