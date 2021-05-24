import pymysql
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from config import host, user, password, db_name
from proxy_auth import proxies

URL1 = 'https://www.rusprofile.ru/codes/89220'
URL2 = 'https://www.rusprofile.ru/codes/429110'
URLs = [
        'https://www.rusprofile.ru/codes/89220',
        'https://www.rusprofile.ru/codes/429110'
]
URL_OKPO = 'https://www.b-kontur.ru/profi/okpo-po-inn-ili-ogrn?inn='  # Адрес для нахождения ОКПО
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36',
    'accept': '*/*'}






def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r

def get_pages_count (html):
    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.find_all('li')
    if pagination:
        return int(pagination[-2].get_text())
    else:
        return 1

def parse_OKPO(INN):
    HEADERS_OKPO = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36',
        'accept': '*/*'}

    def get_html(url, params=None):
        r = requests.get(url, headers=HEADERS_OKPO, params=params)
        return r

    def get_content(html):
        soup = BeautifulSoup(html, 'html.parser')
        r = soup.find(text='ОКПО').find_next('dd').get_text()
        return r
    if str(INN) == 'null':
        return 'null'

    else:
        html = get_html(URL_OKPO + str(INN))
        if html.status_code == 200:
            get_content(html.text)
        else:
            print("Error OKPO")
objects = []
def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='company-item')
    len(items)
    i=0
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
        i += 1
def parse(url):
    html = get_html(url)
    if html.status_code == 200:
        items = []
        pages_count = get_pages_count(html.text)
        for page in range(1, pages_count + 1):
            print(f'Парсинг страницы {page} из {pages_count}')
            html = get_html(url + f'/{page}')
            with concurrent.futures.ThreadPoolExecutor() as executor:
                (executor.map(get_content(html.text), url))
        print(len(objects))
    else:
        print("Error")




parse(URL2)

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