import requests
from bs4 import BeautifulSoup

URL = 'https://www.rusprofile.ru/codes/89220'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36',
    'accept': '*/*'}


def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r


def parse():
    html = get_html(URL)
    print(html)


parse()
