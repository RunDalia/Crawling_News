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
    n_rn = ns1.find('article', attrs={'class': 'article_body_wrap'})
    ## 본문이 다른 태그 아래 있는 경우
    n_body = ns1.find('div', attrs={'class': 'article_txt'})

    ## 이미지 글 제거
    try:
        photos = n_rn.find_all('figure', attrs={'class': 'article_image'})
        for photo in range(len(photos)):
            n_rn.find('figure', attrs={'class': 'article_image'}).decompose()
    except:
        pass

    ## footer 제거
    try:
        footers = n_rn.find_all('div', attrs={'class': 'footer_btnwrap clearfix'})
        for footer in range(len(footers)):
            n_rn.find('div', attrs={'class': 'footer_btnwrap clearfix'}).decompose()
    except:
        pass

    ## 다이아그램 제거
    try:
        diagrams = n_rn.find_all('div', attrs={'class': 'article_image'})
        for diagram in range(len(diagrams)):
            n_rn.find('div', attrs={'class': 'article_image'}).decompose()
    except:
        pass

    ## 관련기사 제거
    try:
        ads = n_rn.find_all('div', attrs={'class': 'related_wrap'})
        for ad in range(len(ads)):
            n_rn.find('div', attrs={'class': 'related_wrap'}).decompose()
    except:
        pass

    ## 쓸데없는 내용 제거
    try:
        ads = n_rn.find_all('span', attrs={'id': 'newsroom_etview_promotion'})
        for ad in range(len(ads)):
            n_rn.find('span', attrs={'id': 'newsroom_etview_promotion'}).decompose()
    except:
        pass

    article_content = []
    content_list = list(n_rn.children)
    count = 0
    contents = []

    for i in range(len(content_list)):
        article_contents = {}
        strongs = {}
        media_summary = {}
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
            if '<h3>' in str(content):
                media_summary['id'] = id + f'.{count}'
                media_summary['type'] = 'media_summary'
                media_list = [re.sub('(<([^>]+)>)', '', i).strip() for i in str(content).split('<br/>')]
                media_list = [i.replace('\xa0', '').replace('\r', '').replace('\t', '').replace('\n', '') for i in
                              media_list]
                media_list = list(filter(None, media_list))
                media_summary['content'] = media_list
                contents.append(media_summary)
                count += 1

    ## 본문이 다른 태그 아래 있는 경우
    for body in n_body:
        article_contents = {}
        strongs = {}
        media_summary = {}
        ## 기본으로 사용하는 if 문
        if type(content) is Comment:
            continue
        elif type(content) == NavigableString:
            continue
        elif type(content) == Tag:
            if content.name == 'br':
                continue

            ##  strong 이 있는 경우 : article_content -> strong 순으로 찍어냄
            if '<strong>' in str(body):
                article_content = [i.replace('\xa0', '').replace('\r', '').replace('\t', '').replace('\n', '') for i in
                                   article_content]
                article_content = list(filter(None, article_content))
                if not article_content:
                    pass
                else:
                    article_contents['id'] = f'{id}.{count}'
                    article_contents['type'] = 'article_content'
                    article_contents['content'] = article_content
                    contents.append(article_contents)
                    count += 1
                    article_content = []

                strong_list = re.sub('(<([^>]+)>)', '', str(body))
                strongs['id'] = f'{id}.{count}'
                strongs['type'] = 'strong'
                strongs['content'] = [strong_list]
                contents.append(strongs)
                count += 1
                article_content = []

            elif '<p>' in str(body):
                bodys = re.sub('(<([^>]+)>)', '', str(body)).split('\n\n')
                article_content.append(bodys)

    article_content = sum(article_content, [])
    article_content = [i.replace('\xa0', '').replace('\r', '').replace('\t', '').replace('\n', '').strip() for i in
                       article_content]
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
        LOGGER.info(f'Etnews_parser {id} End')


if __name__ == '__main__':
    me = singleton.SingleInstance()
    times = datetime.today().strftime("%Y%m%d")

    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'

    loguru.logger.add(
        sink=f'./logs/etnews_parser_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    LOGGER = loguru.logger.bind(name='service')
    LOGGER.info("Etnews parser started")

    db_connection = MongoDB()
    db = db_connection.mongodb_primary()

    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.bind("tcp://*:5030")
    while True:
        data = receiver.recv_pyobj()
        parser(data)
