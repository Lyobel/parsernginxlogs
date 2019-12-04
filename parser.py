# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 13:38:36 2019

@author: belichenko.as
"""
import os
import re
import pandas as pd
import datetime
from atexit import register
from time import ctime
from concurrent.futures import ThreadPoolExecutor
import queue


INPUT_DIR = r"./nginx"
number_threads = 5
lineformat = re.compile(r"""(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>.*?)\] \"(?P<method>(GET|POST)) \/(?P<url>.*?)(\/\d{5}|[?]|[ ]|(\/[a-zA-Z0-9]{8}-))(.*)(HTTP\/1\.1") (?P<statuscode>\d{3}) (?P<answertime>\d+\.\d+) (?P<bytessent>\d+) ["](?P<refferer>.*?)["] ["](?P<useragent>.*?)["] ["](.*)["] ["](?P<proxypass>.+)["]""", re.IGNORECASE)

q = queue.Queue()


def import_data(logfile):
    """ Отправляет для парсинга пул потоков """
    print('Start at:', ctime())
    with ThreadPoolExecutor(number_threads) as executor:
        for l in logfile.readlines():
            executor.submit(parse, l)


def parse(line):
    """ Прогоняет через regex строку и складывает в очередь"""
    data = re.search(lineformat, line)
    if data:
        datadict = data.groupdict()
        # df = pd.DataFrame.from_dict([datadict])
        q.put(datadict)


def convert_df():
    """ Преобразование списка словарей в DataFrame"""
    list_of_dic = []
    while not q.empty():
        list_of_dic.append(q.get())
    df = pd.DataFrame(list_of_dic)
    return df


def transform_datetime(x):
    """ Для конвертации колонки DataFrame в нужный формат времени """
    return datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z')


def _main():
    for f in os.listdir(INPUT_DIR):
        with open(os.path.join(INPUT_DIR, f)) as logfile:
            print('Processing file {}'.format(f))
            import_data(logfile)
            
            print('Size of queue:', q.qsize())
            df = convert_df()

            # Трансформируем поле дата в datetime и делаем индексом
            df['dateandtime'] = df['dateandtime'].apply(lambda x: transform_datetime(x))
            df = df.set_index('dateandtime')

            print('Data Analysis:', ctime())
            # Количество записей
            allrecords = df.size
            # Количество запросов в секунду
            bysec = df.groupby(df.index.time).size().sort_values(ascending=False)[:20]
            # Загрузка в разрезе бэкендов
            byproxypass_all = df.groupby(df['proxypass']).size().sort_values(ascending=False)
            byproxypass_pertime = df.groupby([df.index.time, 'proxypass']).size().sort_values(ascending=False)[:10]
            byproxypass_perurl = df.groupby([df.index.hour, 'proxypass', 'url']).size().sort_values(
                ascending=False)[:10]
            # Топ юзер агентов
            byuseragent = df.groupby(df['useragent']).size().sort_values(ascending=False)[:5]
            # Количество запросов в разрезе минут
            urlbytime = df.groupby([df.index.minute, 'url']).size()
            # Количество запросов в разрезе IP за час
            ipperhour = df.groupby([df.index.hour, 'ipaddress']).size().sort_values(ascending=False)[:10]
            # Количество запросов в разрезе IP/backend
            ipperbackend = df.groupby([df.index.hour, 'ipaddress', 'proxypass']).size().sort_values(
                ascending=False)[:20]
            # Количество запросов в разрезе IP/backend/запрос
            ipperbackendurl = df.groupby([df.index.hour, 'ipaddress', 'proxypass', 'url']).size().sort_values(
                ascending=False)[:20]
            # Статусы
            bystatus = df.groupby([df.index.hour, 'statuscode']).size().sort_values(ascending=False)
            # В разрезе статусов / url
            bystatusurl = df.groupby([df.index.hour, 'statuscode', 'url']).size().sort_values(ascending=False)[:20]
            pd.set_option('display.max_rows', None)

            print('Export Data:', ctime())
            with open('./output/{}_out.txt'.format(f), 'w') as writer:
                print('Все записи', allrecords, file=writer)
                print('\n', file=writer)
                print('Количество запросов в секунду\n', bysec, file=writer)
                print('\n', file=writer)
                print('Загрузка в разрезе бэкендов\n', byproxypass_all, file=writer)
                print('\n', file=writer)
                print('Загрузка в разрезе бэкендов time\n', byproxypass_pertime, file=writer)
                print('\n', file=writer)
                print('Загрузка в разрезе бэкендов url\n', byproxypass_perurl, file=writer)
                print('\n', file=writer)
                print('Топ юзер агентов\n', byuseragent, file=writer)
                print('\n', file=writer)
                print('Количество запросов в разрезе IP за час\n', ipperhour, file=writer)
                print('\n', file=writer)
                print('Количество запросов в разрезе IP/backend\n', ipperbackend, file=writer)
                print('\n', file=writer)
                print('Количество запросов в разрезе IP/backend/запрос\n', ipperbackendurl, file=writer)
                print('\n', file=writer)
                print('Статусы\n', bystatus, file=writer)
                print('\n', file=writer)
                print('В разрезе статусов / url\n', bystatusurl, file=writer)
                print('\n', file=writer)
                print('Количество запросов в разрезе минут\n', urlbytime, file=writer)
                print('\n', file=writer)


if __name__ == '__main__':
    _main()


@register
def _atexit():
    print('All done at:', ctime())
