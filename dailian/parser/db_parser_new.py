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
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
import configparser
import pymongo
import urllib


config = configparser.ConfigParser()
config.read('C://Users//Dev_04//anaconda3//envs//pythonProject//-News_Crawling_Kor//dailian//config//config.conf', encoding='UTF8')
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


'''
media_summary(요약)
strong(소제목)
article_content(본문)
'''
media_summary = {}
strongs = {}
article_contents = {}


def perser(data):
    nsbody = data['body_raw']
    id = data['_id']
    ns1 = BeautifulSoup(nsbody,'lxml')
    ## 최상위 본문 태그
    n_rn = ns1.find('description')

    ## 이미지 글 제거
    try :
        photo = n_rn.find_all('div',attrs={'class':'figure'})
        for photos in range(len(photo)):
            n_rn.find('div',attrs={'class':'figure'}).extract()
    except:
        pass


    article_content = []
    totatl_content = []
    content_list = list(n_rn.children)
    end_idx = len(content_list)
    counx = 0
    contents = []

    for i in range(len(content_list)):
        article_contents = {}
        strongs = {}
        content = content_list[i]

        ## 기본으로 사용하는 if 문
        if type(content) is Comment:
            continue
        elif type(content) == NavigableString:
            article_content.append(content)
        elif type(content) == Tag:
            if content.name == 'br':
                continue

            # print(article_content)
            ## media_summary 첫번째 
            if '<div class="inner-subtitle">' in str(content):
                media_summary['id'] = id+f'.{counx}'
                media_summary['type'] = 'media_summary'
                media_summary['content'] = [re.sub('(<([^>]+)>)', '', i).strip() for i in  str(content).split('<br/>')]
                contents.append(media_summary)
                totatl_content.append(article_content)
                article_content = []
                counx += 1
            
            ##  strong 이 있는 경우 : article_content -> strong 순으로 찍어냄
            elif '<strong>' in str(content):

                totatl_content.append(article_content)
                article_contents['id'] = f'{id}.{counx}'
                article_contents['type'] = 'article_content'
                article_content = list(filter(None, totatl_content))

                ## content 전처리
                try : 
                    article_content = [i[0].replace('\xa0','').replace('\r','').replace('\t','').replace('\n','') for i in article_content]
                    ## 빈공간 삭제
                    article_content = [i.strip() for i in article_content]
                    article_content = list(filter(None, article_content))
                except :
                    pass
                
                article_contents['content'] = article_content
                contents.append(article_contents)
                counx += 1
                article_content = []
                totatl_content = []


                strongs['id'] = f'{id}.{counx}'
                strongs['type'] = 'strong'
                strongs['content'] = [content.text]
                contents.append(strongs)
                counx += 1

                article_content = []
            
            elif '<p>' in str(content):
                totatl_content.append(article_content)
                ## content 전처리
                article_content = content.text.split('<p>')
                article_content = [re.sub('(<([^>]+)>)', '', i).strip() for i in  article_content]
                article_content = [i.replace('\xa0','').replace('\r','').replace('\t','').replace('\n','') for i in article_content]
                ## 빈공간 삭제
                article_content = [i.strip() for i in article_content]
                article_content = list(filter(None, article_content))
    
                
        ## 마지막 문장
        if i+1 == int(end_idx):
            totatl_content.append(article_content)
            article_content = list(filter(None, totatl_content))

            ## content 전처리
            try : 
                article_content = [i[0].replace('\xa0','').replace('\r','').replace('\t','').replace('\n','') for i in article_content]
                ## 빈공간 삭제
                article_content = [i.strip() for i in article_content]
                article_content = list(filter(None, article_content))
            except :
                pass
            article_contents['id'] = f'{id}.{counx}'
            article_contents['type'] = 'article_content'
            article_contents['content'] = article_content
            contents.append(article_contents)
            counx += 1


    data['contents'] = data.pop('body_raw')
    data['contents'] = contents
    # print(data['contents'])
    try:
        collection2.insert_one(data)
        LOGGER.info(f'{id} pre insert')
        sleep(0.1)
    except Exception as error:
        LOGGER.error(error)
    finally:
        LOGGER.info(f'db_perser {id} End')
        

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
    
    ## ID 불러서 쓸경우
    # ped = collection.find({'_id':'1191208998'})
    ## mongodb 전체 불러서 사용할 경우
    ped = collection.find()
    for i in ped:
        perser(i)

    