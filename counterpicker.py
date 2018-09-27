import requests
import time
import json
from pathlib import Path

jsonUrl = 'http://dotapicker.com/assets/json/data/heroadvscores.json'
#Tests seem to suggest 0 is very high, 1 high, 2 normal
skillLevel = 0
cut = 7.0
downloadFile = Path("dotaPickerHeroAdv.json")

if not downloadFile.is_file():
    r = requests.get(jsonUrl, params={'cv': 0})
    time.sleep(3)

    if r.status_code != requests.codes.ok:
        print("Bad status code for {} returned {}".format(r.url,
                                                          r.status_code))
        exit

    with open(downloadFile, 'w') as f:
        f.write(r.text)


def dotaPickerScaler(rate, skillscale):
    return (rate/skillscale * 1000) / 100


with open(downloadFile, 'r') as f:
    js = json.load(f)

    skillScale = js['adv_rates_scale'][skillLevel]
    nameMap = js['heronames']
    res = {k: {'goodvs': set(), 'countered': set()} for k in nameMap}

    for i, winRates in enumerate(js['adv_rates']):
        name = nameMap[i]
        for j, rate in enumerate(winRates):
            if rate is None:
                continue
            if dotaPickerScaler(rate[skillLevel], skillScale) > 7.0:
                opName = nameMap[j]
                res[name]['countered'].add(opName)
                res[opName]['goodvs'].add(name)
    # JSON wont work with sets so convert them
    resList = {}
    for k in res:
        resList[k] = {'goodvs': list(res[k]['goodvs']),
                      'countered': list(res[k]['countered'])}
    outPutFile = Path('processedDotaPicker.json')
    with open(outPutFile, 'w') as f:
        json.dump(resList, f)
