# -*- coding: utf-8 -*-
from tendo import singleton
import loguru
from datetime import datetime
import re
from bs4 import BeautifulSoup
from time import sleep
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from dailian.db_mongo.mongo import MongoDB

import configparser
import pymongo
import urllib


config = configparser.ConfigParser()
config.read('../config/config.conf', encoding='UTF8')
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

def perser(data):
    aid = data['aid']
    data['naver_link'] = f'https://www.dailian.co.kr/news/view/{aid}/' + '?sc=Naver'
    category = data['_id']
    
    
    try : 
        collection.update_one({'_id':data['_id']},{'$set':{'naver_link':data['naver_link']}})
        # print(data['naver_link'])
        LOGGER.info(f'{category} pre insert')
        sleep(0.1)
    except Exception as error:
        LOGGER.error(error)
        
    finally:
        LOGGER.info(f'db_perser {category} End')


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
        perser(i)
    
