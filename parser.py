import requests
from bs4 import BeautifulSoup

URL = 'https://www.rusprofile.ru/codes/89220'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36',
    'accept': '*/*'}


def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r


def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='company-item')
    objects = []
    for item in items:
         objects.append({
             'title': item.find('div', class_="company-item__title").get_text(strip=True),
             'OGRN': item.find('div', class_="company-item-info").get_text(strip=True),
             'OKPO': item.find('div', class_="company-item-info").get_text(strip=True),
             'Status': item.find('div', class_="company-item-info").get_text(strip=True),
             'Date': item.find('div', class_="company-item-info").get_text(strip=True),
             'Capital': item.find('div', class_="company-item-info").get_text(strip=True)
         })
    print(item)
    print(len(objects))
def parse():
    html = get_html(URL)
    if html.status_code == 200:
        get_content(html.text)
    else:
        print("Error")


parse()
