import opendota
import requests

url = "https://api.opendota.com/api/explorer"
sql = open('queryMin.sql', 'r').read()

params = {
    'sql': sql
}
print(params)
r = requests.get(url, params=params)
print(r.json())