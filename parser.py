import pymysql
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from config import host, user, password, db_name
from proxy_auth import proxies

URL1 = 'https://www.rusprofile.ru/codes/89220'
URL2 = 'https://www.rusprofile.ru/codes/429110'

URL_OKPO = 'https://www.b-kontur.ru/profi/okpo-po-inn-ili-ogrn?inn='  # Адрес для нахождения ОКПО
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36',
    'accept': '*/*'}

#Получает колличество страниц в списке
def get_pages_count(html):
    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.find_all('li')
    if pagination:
        return int(pagination[-2].get_text())
    else:
        return 1


def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r

#функция получения ОКПО через ИНН, через сторонний сайт. Не используется, так как не может обрабатывать много запросов
def get_OKPO(INN):
    HEADERS_OKPO = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Mobile Safari/537.36',
        'accept': '*/*'}

    def get_html(url):
        # r = requests.get(url, headers=HEADERS_OKPO, proxies=proxies)
        r = requests.get(url, headers=HEADERS_OKPO)
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
            return get_content(html.text)
        else:
            print("Error OKPO")


objects = []

#Функция получения контента
def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='company-item')
    len(items)
    i = 0
    #Специальное условие для статуса организации, так как для разных статусов используется разный стиль, либо статуса просто нет
    for item in items:
        Status = item.find('span', class_="attention-text") or item.find('span', class_="warning-text")
        if Status:
            Status = Status.get_text()
        else:
            Status = 'Действующая'
        #Функция проверки пустых значений
        def null_value(value):
            if item.find(text=value):
                return item.find(text=value).find_next('dd').get_text()
            else:
                return 'null'
        #Получение и запись в словарь данных с сайта
        objects.append({
            'title': item.find('div', class_="company-item__title").get_text(strip=True),
            'OGRN': null_value('ОГРН'),
            'INN': null_value('ИНН'),
            'Status': Status,
            'Date': null_value('Дата регистрации'),
            'Capital': null_value('Уставный капитал')
        })
        # objects.append({
        # 'OKPO': get_OKPO(objects[i]['INN']),
        # })
        i += 1

#Функция парсера
def parse(url):
    #Функция для получения имени базы данных
    def get_name_db(url):
        def get_html(url, params=None):
            r = requests.get(url, headers=HEADERS, params=params)
            return r

        def get_content(html):
            soup = BeautifulSoup(html, 'html.parser')
            r = soup.find('div', class_='content-frame__information').get_text()
            return r

        html = get_html(url)
        if html.status_code == 200:
            return get_content(html.text)
        else:
            print("Error Имя базы данных")
    name_bd = get_name_db(url)
    print(f'Парсинг страницы {name_bd}')
    html = get_html(url)
    #Непосредственно парсинг сайта
    if html.status_code == 200:
        pages_count = get_pages_count(html.text)
        for page in range(1, pages_count + 1):
            print(f'Парсинг страницы {page} из {pages_count}')
            html = get_html(url + f'/{page}')
            #Использую метод map модуля concurrent.futures для вызова функции в отдельном потоке (макс. 5)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                (executor.map(get_content(html.text), url))
        print(f'Найдено и обработано {len(objects)} объектов')
        #Проверяю подключение к базе данных
        try:
            connection = pymysql.connect(
                host=host,
                port=3306,
                user=user,
                password=password,
                database=db_name,
                cursorclass=pymysql.cursors.DictCursor
            )
            print("Подключение установлено")

            try:
                with connection.cursor() as cursor:
                    #Создаю таблицу, если ее нет, для конкретного кода ОКВЭД
                    create_table_query = f"CREATE TABLE IF NOT EXISTS`{name_bd}`(id int AUTO_INCREMENT," \
                                         " Title_comp varchar(32)," \
                                         " OGRN varchar(32)," \
                                         " INN varchar(32)," \
                                         " Status varchar(64)," \
                                         " Date_reg varchar (32)," \
                                         " Capital varchar(32), PRIMARY KEY (id));"
                    cursor.execute(create_table_query)
                    print(f"Таблица \"{name_bd}\" создана или была создана раньше")
                    #Для поддержания актуальных данных в таблице, все старые данные удаляются
                    delete_table_query = f"DELETE FROM`{name_bd}`;"
                    cursor.execute(delete_table_query)
                    connection.commit()
                    #Запись данных в таблицу
                    for item in objects:
                        a = ()
                        a = (str(item['title']), str(item['OGRN']), str(item['INN']), str(item['Status']), str(item['Date']), str(item['Capital']),)
                        insert_query = f"INSERT INTO `{name_bd}` (`Title_comp`, `OGRN`, `INN`, `Status`, `Date_reg`, `Capital`) VALUES (%s, %s, %s, %s, %s, %s);"
                        cursor.execute(insert_query, a)
                        connection.commit()
                    print(f'Данные со страницы {name_bd} записаны в базу данных')
            finally:
                connection.close()
        except Exception as ex:
            print('В соединении отказано')
            print(ex)
    else:
        print("Error")


parse(URL1)
parse(URL2)
