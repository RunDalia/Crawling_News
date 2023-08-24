# -*- coding: utf-8 -*-
from tendo import singleton
import loguru
from datetime import datetime
import re
from bs4 import BeautifulSoup, NavigableString, Tag, Comment
import zmq

import sys, os

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db_mongo.mongo import MongoDB

'''
media_summary(요약)
strong(소제목)
article_content(본문)
'''
media_summary = {}
strongs = {}
article_contents = {}


def parser(data):
    nsbody = data['body_raw']
    id = data['_id']
    ns1 = BeautifulSoup(nsbody, 'lxml')
    n_rn = ns1.find('article', attrs={'class': 'viewBox2'})

    ## 이미지 글 제거
    try:
        photos = n_rn.find_all('figure', attrs={'class': 'class_div_main image'})
        for photo in range(len(photos)):
            n_rn.find('figure', attrs={'class': 'class_div_main image'}).decompose()
    except:
        pass

    ## 이미지 글 제거 2
    try:
        images = n_rn.find_all('figure', attrs={'class': 'image'})
        for image in range(len(images)):
            n_rn.find('figure', attrs= {'class': 'image'}).decompose()
    except:
        pass

    article_content = []
    contents = []
    count = 0
    total_content = []
    content_list = list(n_rn.children)

    for i in range(len(content_list)):
        media_summary = {}
        article_contents = {}
        strongs = {}
        content = content_list[i]

        ## 기본으로 사용하는 if 문
        if type(content) is Comment:
            continue
        elif type(content) == NavigableString:
            continue
        elif type(content) == Tag:
            if content.name == 'br':
                continue

            ## media_summary 첫번째
            if '<em class="precis">' in str(content):
                media_summary['id'] = id + f'.{count}'
                media_summary['type'] = 'media_summary'
                media_list = [re.sub('(<([^>]+)>)', '', i).strip() for i in str(content).split('<br/>')]
                media_list = [i.replace('\xa0', '').replace('\r', '').replace('\t', '').replace('\n', '') for i in
                              media_list]
                media_list = list(filter(None, media_list))
                media_summary['content'] = media_list
                contents.append(media_summary)
                count += 1

            ##  strong 이 있는 경우 : article_content -> strong 순으로 찍어냄
            if '<strong>' in str(content):
                article_contents['id'] = f'{id}.{count}'
                article_contents['type'] = 'article_content'
                article_content = list(filter(None, total_content))

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
                count += 1
                article_content = []

                strongs['id'] = f'{id}.{count}'
                strongs['type'] = 'strong'
                strongs['content'] = [content.text]
                contents.append(strongs)
                count += 1

                article_content = []

            if '<p>' in str(content):
                content = re.sub('(<([^>]+)>)', '', str(content)).split('</p>')
                article_content.append(content)
    article_content = sum(article_content, [])
    article_content = [i.replace('\xa0', '').replace('\r', '').replace('\t', '').replace('\n', '').replace('&lt;', '').replace('&gt;', '').strip()
                       for i in article_content]
    article_content = list(filter(None, article_content))

    ## 마지막 문장 진행
    if not article_content:
        pass
    else:
        article_contents['id'] = f'{id}.{count}'
        article_contents['type'] = 'article_content'
        article_contents['content'] = article_content
        contents.append(article_contents)

    data['contents'] = data.pop('body_raw')
    data['contents'] = contents

    try:
        db_connection.mongodb_connection_pre(data, db)
        LOGGER.info(f'{id} pre insert')

    except Exception as error:
        LOGGER.error(error)

    finally:
        LOGGER.info(f'Segye_parser {id} End')


if __name__ == '__main__':
    me = singleton.SingleInstance()
    times = datetime.today().strftime("%Y%m%d")

    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'

    loguru.logger.add(
        sink=f'./logs/segye_parser_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    LOGGER = loguru.logger.bind(name='service')
    LOGGER.info("Segye parser started")

    db_connection = MongoDB()
    db = db_connection.mongodb_primary()

    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://*:5022")
    while True:
        data = receiver.recv_pyobj()
        parser(data)
