# -*- coding: utf-8 -*-
from tendo import singleton
import loguru
from datetime import datetime
import re
from bs4 import BeautifulSoup
from time import sleep
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db_mongo.mongo import MongoDB

import configparser
import pymongo
import urllib


config = configparser.ConfigParser()
config.read('./config/config.conf', encoding='UTF8')
config_mob = config['MongoDB']
port = config_mob['port']
host1 = config_mob['hostname1']
username = urllib.parse.quote_plus(config_mob['username'])
password = urllib.parse.quote_plus(config_mob['password'])
conn = pymongo.MongoClient(f'mongodb://{username}:{password}@{host1}:{port}/')
db = conn[config_mob['db_name']]
collection = db[config_mob['table_name']]
db2 = conn[config_mob['db_name2']]
collection2 = db2[config_mob['table_name2']]


def parser(data):
    list1 = {}
    tit = {}
    main = {}
    nsbody = data['body_raw']
    ns1 = BeautifulSoup(nsbody, 'lxml')
    n_rn = ns1.find('div', attrs={'class':'con_left'})

    ## 강조
    try :
        n_as = n_rn.find('div',attrs={'class':'article_summary'}).text.strip().split('\n')
        for i in range(len(n_as)):
            a = str(i)
            tit[a] = n_as[i]
        list1['subtitle'] = tit
    except:
        pass

    ## 본문
    n_body = n_rn.find('div',attrs={'class':'article_view'})
    try:
        n_figure = n_body.find_all('figure')
        for fig in range(len(n_figure)):
            n_body.find('figure').extract()
    except:
        pass
    try:
        n_div = n_body.find_all('div')
        for div in range(len(n_div)):
            n_body.find('div').extract()
    except:
        pass
    try:
        n_span = n_body.find_all('span')
        for span in range(len(n_span)):
            n_body.find('span').extract()
    except:
        pass
    try:
        n_body.find('div',attrs={'class':'article_copy'}).extract()
    except:
        pass
    nnnn = str(n_body).replace('<br/>','\n').strip()
    nnnn1 = re.sub('(<([^>]+)>)', '', nnnn).split('\n')
    po = 0
    for j in range(len(nnnn1)):
        if nnnn1[j] == '':
            pass
        else:
            p = str(po)
            main[p] = nnnn1[j]
            po += 1
    list1['main'] = main

    data['content'] = data.pop('body_raw')
    data['content'] = list1
    ids = data['_id']
    
    
    try : 
        collection2.insert_one(data)
        # print(data)
        LOGGER.info(f'{ids} pre insert')
        sleep(0.1)
    except Exception as error:
        LOGGER.error(error)
        
    finally:
        LOGGER.info(f'db_perser {ids} End')
        

if __name__ == '__main__':
    me = singleton.SingleInstance()
    times = datetime.today().strftime("%Y%m%d")
    
    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'
    
    loguru.logger.add(
        sink=f'./logs/db_perser_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    LOGGER = loguru.logger.bind(name='service')
    LOGGER.info("db perser started")

    ped = collection.find()
    for i in ped:
        parser(i)

    