# -*- coding: utf-8 -*-
import json

import loguru
from tendo import singleton
from time import sleep
from datetime import datetime
import datetime as dt
import requests
import re
from bs4 import BeautifulSoup
import zmq
import sys, os

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from db_mongo.mongo import MongoDB


class ETNews:
    """ 전자신문 뉴스 크롤러 """

    def __init__(self):
        self.header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
        self.db_connection = MongoDB()
        self.conn = self.db_connection.mongodb_primary()
        self.LOGGER = loguru.logger.bind(name='service')
        self.cate_list = self.category_list()[0]
        self.sub_list = self.category_list()[1]
        self.socket = self.zmq_connect()

    def zmq_connect(self):
        context = zmq.Context()
        perser = context.socket(zmq.PUSH)
        perser.connect("tcp://127.0.0.1:5030")
        return perser

    def category_list(self) -> dict:
        category_list = {'경제·금융': '02', '전자·모빌리티': '17', '통신·미디어·게임': '03', '소재·부품': '06', 'SW·보안': '04',
                         '산업·에너지·환경': '20', '플랫폼·유통': '60', '벤처·바이오': '16', '정치': '22', '전국': '25',
                         '국제': '12', '골프': '15'}
        subcategory_list ={}
        subcategory_list['경제·금융'] = {'증권': '021', '경제': '024', '금융': '027', '특허': '022'}
        subcategory_list['전자·모빌리티'] = {'전자산업': '063', '모빌리티': '066'}
        subcategory_list['통신·미디어·게임'] = {'통신': '033', '방송': '025', '네트워크': '225', '게임': '104', '콘텐츠': '102'}
        subcategory_list['소재·부품'] = {'소재': '064', '부품': '062', '장비': '061'}
        subcategory_list['SW·보안'] = {'SW': '043', 'SW교육': '044', '보안': '045'}
        subcategory_list['산업·에너지·환경'] = {'과학': '020', '에너지': '065'}
        subcategory_list['플랫폼·유통'] = {'유통': '068', '기업': '028'}
        subcategory_list['벤처·바이오'] = {'의료바이오': '042', '중기/벤처': '069'}
        subcategory_list['정치'] = {'정책': '210', '정치': '220', '교육': '230'}
        subcategory_list['전국'] = {'전국': '023'}
        subcategory_list['국제'] = {'국제': ''}
        subcategory_list['골프'] = {'골프': ''}

        return category_list, subcategory_list

    def url_list(self):
        cate_key = [i for i in self.cate_list.keys()]  # category(kor)
        cate_values = [i for i in self.cate_list.values()]  # category(eng)
        for i in range(len(cate_values)):
            sub_key = [j for j in self.sub_list[cate_key[i]]]  # subcategory(kor) list
            sub_values = [j for j in self.sub_list[cate_key[i]].values()]    # subcategory(num) list
            for k in range(len(sub_key)):
                subkey_list = sub_key[k]    # subcategory(kor)
                subvalue_list = sub_values[k]   # subcategory(num)
                url_list = f'https://www.etnews.com/news/section.html?id1={cate_values[i]}&id2={subvalue_list}'
                self.LOGGER.info(url_list)
                try:
                    res = requests.get(url_list, headers=self.header)
                    soup = BeautifulSoup(res.content, 'lxml')
                    temp = soup.find('section', attrs={'class': 'main_news_wrap'})
                    temp_list = temp.find_all('article', attrs={'class': 'main_article'})
                    self.insert_mongo(temp_list, cate_key[i], cate_values[i], subkey_list, subvalue_list)
                except Exception as ee:
                    self.LOGGER.error(ee)
        return i

    def crawler(self, news, cate_key, cate_values, subkey_list, subvalue_list) -> dict:
        etnews_list = {}
        self.LOGGER.info(f"etnews {cate_key}, {subkey_list} started...")
        href = news.find('a').get('href')
        aid = href.split('/')[-1]
        etnews_list['oid'] = '030'   # etnews oid
        etnews_list['_id'] = '030' + aid
        etnews_list['sid'] = cate_values
        etnews_list['sid2'] = subvalue_list
        etnews_list['aid'] = aid
        etnews_list['headline'] = news.find('p').get_text().strip()
        etnews_list['summary'] = None
        etnews_list['category'] = cate_key
        etnews_list['subcategory'] = subkey_list
        etnews_list['press'] = '전자신문'
        try:
            etnews_list['img_url'] = news.find('img').get('src')
        except:
            etnews_list['img_url'] = None
        etnews_list['content_url'] = 'https://www.etnews.com' + href
        etnews_list['naver_link'] = etnews_list['content_url']
        sleep(3)

        ## body_raw
        body_res = requests.get(etnews_list['content_url'], headers=self.header)
        body_soup = BeautifulSoup(body_res.content, 'lxml')
        etnews_list['body_raw'] = f"{body_soup}"

        ## 기자 이름
        try:
            reporter = body_soup.find('meta', attrs={'property': 'dable:author'}).get('content')
            if reporter == '':
                article_body = body_soup.find('div', attrs={'class': 'article_txt'}).find_all('p')
                reporter_info = article_body[-1].text.strip().split('\n')[-1]
                regex = "\(.*\)|\s\s.*"
                email_pattern = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9_.]+)'
                reporter = re.sub(regex, '', reporter_info).strip()
                reporter = re.sub(pattern=email_pattern, repl='', string=reporter).strip()
                if len(reporter) > 20:
                    reporter = '사설 기사'
            reporter = reporter.replace('전자신문인터넷 ', '')
            etnews_list['author'] = reporter
        except:
            reporter = body_soup.find('meta', attrs={'name': 'news_keywords'}).get('content')
            etnews_list['author'] = reporter

        date = body_soup.find('time', attrs={'class':'date'}).get_text()
        date = date.split(' : ')[-1]
        etnews_list['article_date'] = dt.datetime.strptime(date, '%Y-%m-%d %H:%M').strftime('%Y.%m.%d %H:%M')
        etnews_list['article_editdate'] = etnews_list['article_date']
        etnews_list['news_date'] = dt.datetime.strptime(etnews_list['article_date'], '%Y.%m.%d %H:%M').strftime('%Y%m%d')
        etnews_list['insertTime'] = str(datetime.now())
        return etnews_list

    def mongo_data(self, etnews_list) -> dict:
        mongo_data = {'_id': etnews_list['_id'],
                      'sid': etnews_list['sid'],
                      'sid2': etnews_list['sid2'],
                      'oid': etnews_list['oid'],
                      'aid': etnews_list['aid'],
                      'news_date': etnews_list['news_date'],
                      'category': etnews_list['category'],
                      'subcategory': etnews_list['subcategory'],
                      'press': etnews_list['press'],
                      'headline': etnews_list['headline'],
                      'summary': etnews_list['summary'],
                      'body_raw': etnews_list['body_raw'],
                      'article_date': etnews_list['article_date'],
                      'article_editdate': etnews_list['article_editdate'],
                      'author': etnews_list['author'],
                      'content_url': etnews_list['content_url'],
                      'naver_link' : etnews_list['naver_link'],
                      'img_url': etnews_list['img_url'],
                      'import_result': 'N',
                      'search_result': 'N',
                      'insertTime': etnews_list['insertTime'],
                      }
        return mongo_data

    def insert_mongo(self, all_news, cate_key, cate_values, subkey_list, subvalue_list):
        count = 0
        for news in all_news:
            etnews_list = self.crawler(news, cate_key, cate_values, subkey_list, subvalue_list)
            mongo_data = self.mongo_data(etnews_list)
            ## 중복 처리
            try:
                # print(mongo_data)
                self.db_connection.mongodb_connection(mongo_data, self.conn)
                # self.socket.send_pyobj(mongo_data)
                self.LOGGER.info('etnews_news Mongo Insert : ' + mongo_data['_id'])
                # 정상적으로 삽입이 완료되면 count 0으로 초기화 (중복으로 연속으로 count하기 위함)
                count = 0
                sleep(3)
            except Exception as ee:
                self.LOGGER.error(f'data duplicated : {mongo_data["_id"]}')
                count += 1
                print('count : ', count)
                if count == 3:
                    return news


def main():
    start = ETNews()
    start.url_list()


if __name__ == '__main__':
    me = singleton.SingleInstance()

    times = datetime.today().strftime("%Y%m%d")
    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'
    loguru.logger.add(
        sink=f'./logs/etnews_Main_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    main()