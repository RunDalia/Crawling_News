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

def perser(data):
    list1 = []
    nsbody = data['body_raw']
    ns1 = BeautifulSoup(nsbody, 'lxml')
    n_rn = ns1.find('div', attrs={'class':'cont_art'})
    n_div = n_rn.find_all('div')
    reporter_nm = n_rn.find('span',attrs={'id':'customByline'}).text
    for div in range(len(n_div)):
        n_rn.find('div').extract()
    n_rn.find('span',attrs={'id':'customByline'}).extract()
    n_rn.find('p',attrs={'class':'art_copyright'}).extract()
    for text in n_rn:
        new_text = text.text.replace('* ','').replace(' = ',' - ').replace('# ','').strip()
        new_text = new_text.replace('\r\n','').replace('\t\t\t\t\t\t\t\t\n','')
        ## http or https 제거
        new_text = re.sub(r'[a-z]+://[a-z0-9.-_]+','',new_text).replace('()','')
        ## 이메일 제거
        new_text = re.sub(r'[a-z]+@[a-z0-9.-_]+.com','',new_text).replace('()','')
        new_text = re.sub(r'[a-z]+@[a-z0-9.-_]+.co.kr','',new_text).replace('()','')
        ## 문자열 사이의 불필요한 unicode를 제거하십시오.
        new_text1 = new_text.replace(r'\xa0', r' ') \
            .replace(r'\u200b', r'').replace(r'\u3000', r'  ') \
            .replace(r'\ufeff', r'').replace(r'\ue3e2', r'..') \
            .replace(r'\x7f', r'').replace(r'\u2009', r' ') \
            .replace(r'\xad', r' ').replace(r'\uec36', r'...')
            
        ## unknown token    
        unk_t1 = unknwon_token_replace(new_text1)
        unk_text1 = remove_left_string(unk_t1)
        
        list1.append(unk_text1)
        
    listx = ''.join(list1)
    data['content'] = data.pop('body_raw')
    data['content'] = listx.strip()
    data['summary'] = listx[:300]
    data['author'] = re.sub(r'[^가-힇]','',reporter_nm).replace('@','').strip()
    data['import_result'] = 'N'
    data['search_result'] = 'N'
    ids = data['_id']
    
    
    try : 
        # collection2.insert_one(data)
        print(data)
        LOGGER.info(f'{ids} pre insert')
        sleep(0.1)
    except Exception as error:
        LOGGER.error(error)
        
    finally:
        LOGGER.info(f'db_perser {ids} End')
        
        
# unknown token을 문자로 치환
unknown_token_list = [
    ['比', '대비 '],
    ['韓', '한국 '],
    ['美', '미국 '],
    ['日', '일본 '],
    ['株', '주'],
    ['證', '증권'],
    ['銀', '은행'],
    ['…', ', '],
    ['`', '\''],
    ['“', '\"'],
    ['”', '\"'],
    ['‘', '\''],
    ['’', '\''],
    ['↑', '상승'],
    ['↓', '하락'],
    ['△', ','],
    ['▲', ','],
    ['▶', ','],
    ['▷', ','],
    ['→', ','],
    ['ㆍ', ','],
    ['百', '백화점'],
    ['重', '중공업'],
    ['外','외']
]        
pattern_unknown_token_replace = [re.compile(unknown_token[0]) for unknown_token in unknown_token_list]  
def unknwon_token_replace(text):
    for index, pattern_func in enumerate(pattern_unknown_token_replace):
        text = pattern_func.sub(unknown_token_list[index][1], text)
    return text

remove_left_string_list = [
    r'기자 =',
    r'기자］',
    r'기자] ',  # 위의 것과 다른 경우임
    r'기자]',
    r'기자\) ',
    r'기자',
    r'기자 ',
    r'재배포 금지',
    r'배포금지',
    r'\[파이낸셜뉴스\]',
    r'/사진(.+) ',
    r'\[사진(.+)\]',
    r'\[사진\] (.+)',
    r'\[사진\](.+)',
    r'\[사진 =(.+)\]',
    r'\(사진제공=(.+)\)',
    r'\(사진=(.+)\)',
    r'사진\|(.+)',
    r'사진 \|(.+)',
    r'사진=(.+)',
    r'사진 = (.+)'
]
pattern_remove_left_string_list = [re.compile(remove_left_string) for remove_left_string in remove_left_string_list]
def remove_left_string(text):
    for index, pattern_func in enumerate(pattern_remove_left_string_list):
        matchObj = pattern_func.search(text)
        if matchObj == None:
            continue
        text = text[matchObj.end():]
    return text

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
    
    # db_connetion = MongoDB()
    # db = db_connetion.mongodb_primary()
    
    # context = zmq.Context()
    # receiver = context.socket(zmq.PULL)
    # receiver.bind("tcp://*:5277")
    # while True:
    #     data = receiver.recv_pyobj()
    #     perser(data)
    