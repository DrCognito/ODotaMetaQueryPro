from player import getHeroResults, getFilteredHeroResults, Player
from player import getHeroResultsTime
from replay import Replay
from database import getSession
from pandas import DataFrame
import matplotlib.pyplot as plt
import datetime
#from math import sqrt
from numpy import sqrt

results = DataFrame()
results30 = DataFrame()
results7 = DataFrame()

now = datetime.datetime.now()
Past30 = now - datetime.timedelta(days=30)
Past7 = now - datetime.timedelta(days=7)

session = getSession()
#

for hero_id in session.query(Player.hero_id).distinct():
    heroFilter = (Player.hero_id == hero_id[0],)
    replay30Filter = (Replay.start_time >= Past30,)
    replay7Filter = (Replay.start_time >= Past7, )

    h = getHeroResults(session, hero_id[0])
    h30 = getFilteredHeroResults(session, hero_id[0], heroFilter,
                                 replay30Filter)
    h7 = getFilteredHeroResults(session, hero_id[0], heroFilter, replay7Filter)

    results[h['Name']] = h
    results30[h30['Name']] = h30
    results7[h7['Name']] = h7


def figOut(table, path):
    table = table.T
    table = table.sort_values(by=['Win Rate', 'Name'])
    table['Win Rate'].plot.barh(figsize=(9, 11), xlim=(0.2, 0.8),
                                xerr=table['Stat. Error'])
    plt.tight_layout()

    fig = plt.gcf()
    fig.savefig(path, bbox_inches='tight')


def dualAxis(table, path):
    fig, ax1 = plt.subplots()
    table = table.T
    table = table.sort_values(by=['Win Rate', 'Name'])

    table['Win Rate'][0:25].plot(figsize=(9, 11), ax=ax1)
    ax2 = ax1.twinx()
    table['Picks'][0:25].plot.bar(ax=ax2, alpha=0.5)


    fig.savefig(path,  bbox_inches='tight')


figOut(results, "winrate.png")
figOut(results30, "winrate_30.png")
figOut(results7, "winrate_7.png")

dualAxis(results, "dualWin.png")
dualAxis(results30, "dualWin30.png")
dualAxis(results7, "dualWin7.png")


def outFile(table, file):
    with open(file, mode="w") as f:
        f.write(table.T.to_csv())


outFile(results, "winrate.txt")
outFile(results30, "winrate_30.txt")
outFile(results7, "winrate_7.txt")

#Lycan
lycan = getHeroResultsTime(session, 77)
lycan = lycan.resample('M').sum()

dark_seer = getHeroResultsTime(session, 55)
dark_seer = dark_seer.resample('M').sum()

sf = getHeroResultsTime(session, 11)
sf = sf.resample('M').sum()

lina = getHeroResultsTime(session, 25)
lina = lina.resample('M').sum()

def plotConfLine(table, axis, colour="red", alpha=0.5):
    table['WinRate'] = table['Win'] / (table['Loss'] + table['Win'])
    table['Error'] = 1 / sqrt(table['Win'] + table['Loss'])
    table['MinRate'] = table['WinRate'] - table['Error'] * table['WinRate']
    table['MaxRate'] = table['WinRate'] + table['Error'] * table['WinRate']
    
    table.plot(y="WinRate", ax=axis, color=colour)
    #print(table)
    axis.fill_between(table.index, table['MinRate'], table['MaxRate'],
                      facecolor=colour, alpha=alpha, interpolate=True)



fig, axis = plt.subplots()
plotConfLine(lycan, axis=axis, colour='red')
plotConfLine(dark_seer, axis=axis, colour='green')
plotConfLine(sf, axis=axis, colour='blue')
plotConfLine(lina, axis=axis, colour='yellow')
axis.legend(["Lycan", "Dark Seer", "ShadowFiend", "Lina"])