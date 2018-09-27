import math
import json
import time
from pathlib import Path

import requests
from sqlalchemy import Column, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import scipy.stats as st
from heroes import heroByID, SheetHeroMap

Base = declarative_base()
jsonCache = 'jsonCache/'
apiUrl = 'https://api.opendota.com/api/heroes/\{hero\}/matchups'


class HeroWinRate(Base):
    __tablename__ = "winrate"
    hero = Column(Integer, primary_key=True)
    opponent = Column(Integer, primary_key=True)
    games_played = Column(Integer)
    wins = Column(Integer)


engine = create_engine('sqlite:///odota_winrate.db', echo=False)
# Make all our tables
Base.metadata.create_all(engine)


def getSession():
    Session = sessionmaker(bind=engine)
    return Session()


# https://gist.github.com/loisaidasam/4e174fb9f56b05ae549b7b5798cc7f90
def get_z(confidence):
    z = st.norm.ppf(1 - (1 - confidence) / 2)
    return z


def ci_lower_bound(pos, n, confidence=None, z=None):
    if n == 0:
        return 0
    if z is None:
        z = get_z(confidence)
    phat = 1.0 * pos / n
    return (phat + z * z / (2 * n) - z * math.sqrt((phat * (1 - phat) + z * z /
            (4 * n)) / n)) / (1 + z * z / n)


# https://corplingstats.wordpress.com/2012/04/03/plotting-confidence-intervals/
def wilson_lower_continuous(pos, n, confidence=None, z=None):
    if n == 0:
        return 0
    if z is None:
        z = get_z(confidence)

    # Required for continuous wilsons
    if pos == 0:
        return 0
    p = 1.0 * pos/n
    a = z*z - 1.0/n + 4.0*n*p*(1-p)
    b = 2 - 4*p
    d = 2*(n + z*z)
    s = (math.sqrt(a-b) + 1) / d
    return max(0, p - z*s)


for h in heroByID:
    cache = Path(jsonCache, str(h) + '.json')

    if not cache.is_file():
        reqUrl = apiUrl.replace("\{hero\}", str(h))
        # print(reqUrl, cache)
        r = requests.get(reqUrl, params={'cv': 0})
        time.sleep(3)

        if r.status_code != requests.codes.ok:
            print("Bad status code for {} returned {}".format(r.url,
                                                              r.status_code))
            exit

        with open(cache, 'w') as f:
            f.write(r.text)
    session = getSession()
    with open(cache, 'r') as f:
        
        jsfile = json.load(f)
        for res in jsfile:
            result = HeroWinRate()
            result.hero = h
            result.games_played = res['games_played']
            result.opponent = res['hero_id']
            result.wins = res['wins']

            try:
                session.merge(result)
            except SQLAlchemyError:
                session.rollback()
                print(result)
                raise
    session.commit()

zVal = get_z(0.68)
res = {SheetHeroMap[k]: {} for k in SheetHeroMap}
session = getSession()
for h in heroByID:
    name = SheetHeroMap[heroByID[h]]
    for oh in session.query(HeroWinRate).filter_by(hero=h):
        opponent = SheetHeroMap[heroByID[oh.opponent]]
        # adv = ci_lower_bound(oh.wins, oh.games_played, confidence=0.6827)
        confidence = 0.6827 if 2*oh.wins >= oh.games_played else 0.9545
        adv = wilson_lower_continuous(oh.wins, oh.games_played, confidence=confidence)
        # adv2 = wilson_lower_continuous(oh.wins, oh.games_played, confidence=0.9545)
        # print(adv, adv2)
        adv = round(adv, 2)
        res[name][opponent] = adv

jsonOut = Path('odotaAdvantages.json')
with open(jsonOut, 'w') as f:
    json.dump(res, f)

# h = 74
# invoker_abaddon = session.query(HeroWinRate).filter_by(hero=74, opponent=1).one()
# print("Advantage: ", ci_lower_bound(invoker_abaddon.wins,
#                                     invoker_abaddon.games_played,
#                                     confidence=0.6827))
