import pymysql
from config import host, user, password, db_name

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
    len(items)
    for item in items:
        #print (item)
        Status = item.find('span', class_="attention-text") or item.find('span', class_="warning-text")
        if Status:
             Status = Status.get_text()
        else:
             Status = 'Действующая'
        def null_value(value):
            if item.find(text=value):
                return item.find(text=value).find_next('dd').get_text()
            else:
                return 'null'
        objects.append({
            'title': item.find('div', class_="company-item__title").get_text(strip=True),
            'OGRN': null_value('ОГРН'),
            'INN': null_value('ИНН'),
            'Status': Status,
            'Date': null_value('Дата регистрации'),
            'Capital': null_value('Уставный капитал')
         })
    print(objects)
    print(len(objects))
def parse():
    html = get_html(URL)
    if html.status_code == 200:
        get_content(html.text)
    else:
        print("Error")


parse()

try:
    connection = pymysql.connect(
        host=host,
        port=3306,
        user=user,
        password=password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    print('successfully connected...')
except Exception as ex:
    print('conection refused...')
    print(ex)
