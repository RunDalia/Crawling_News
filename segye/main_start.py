# -*- coding: utf-8 -*-
import json
import textwrap

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


class Segye:
    """ 세계일보 뉴스 크롤러 """

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
        perser.connect("tcp://127.0.0.1:5022")
        return perser

    def category_list(self) -> dict:
        category_list = {'정치': '010101', '사회': '010108', '문화': '010105', '전국': '010107', '피플': '010112',
                         '오피니언': '010110', '비즈': '010103', '연예': '010106', '스포츠': '010111'}
        subcategory_list ={}
        subcategory_list['정치'] = {'일반': '0100000', '대통령실': '0200000', '국회·정당': '0400000', '선거 · 선관위': '0500000',
                                  '외교 · 안보': '0600000', '국방': '0700000', '북한 · 통일': '0800000'}
        subcategory_list['사회'] = {'일반': '0100000', '검찰 · 법원': '0300000', '노동 · 복지': '0500000',
                                  '환경 · 날씨': '0700000', '교통 · 항공': '0800000', '교육 · 학교': '0900000', '사건사고': '01000000'}
        subcategory_list['문화'] = {'일반': '0100000', '미디어': '0300000', '종교 · 학술': '0400000', '음악 · 공연': '0600000',
                                  '미술 · 전시': '0700000', '건강 · 의료': '0800000', '관광 · 레저': '0900000',
                                  '문학 · 출판': '01000000', '음식': '01100000', '패션 · 뷰티': '01300000', '결혼 · 육아': '01400000'}
        subcategory_list['전국'] = {'일반': '0100000', '서울': '1000000', '경기': '0900000', '강원': '1100000',
                                  '충청': '0600000', '영남': '1200000', '호남': '0700000', '제주': '0800000'}
        subcategory_list['피플'] = {'일반': '0100000', '동정 · 인사': '0300000', '부음': '0200000', '인터뷰': '0500000'}
        subcategory_list['오피니언'] = {'사설': '0300000', '기자 · 데스크': '0503000', '논설위원칼럼': '0501000',
                                    '전문가칼럼': '0504000', '외부칼럼': '0502000', '기고 · 독자': '0800000', '월드': '0200000'}
        subcategory_list['비즈'] = {'일반': '0100000', '금융 · 증권': '0300000', '보험': '0500000', '부동산 · 건설': '0700000',
                                  'IT · 과학': '0900000', '산업 · 기업': '1100000', '자동차': '1200000', '쇼핑 · 유통': '1500000',
                                  '취업 · 창업': '1600000', '재테크': '1700000'}
        subcategory_list['연예'] = {'연예가소식': '0100000', 'TV · 방송': '0500000', '영화': '0300000',
                                  '대중음악': '0400000', '해외연예': '0700000'}
        subcategory_list['스포츠'] = {'일반': '0100000', '야구': '0400000', '축구': '0300000', '농구': '0500000',
                                   '배구': '0600000', '골프': '0200000'}

        return category_list, subcategory_list

    def url_list(self):
        cate_key = [i for i in self.cate_list.keys()]  # category(kor)
        cate_values = [i for i in self.cate_list.values()]  # category(num)
        for i in range(len(cate_values)):
            subkey_list = [j for j in self.sub_list[cate_key[i]]]  # subcategory(kor) list
            subvalue_list = [j for j in self.sub_list[cate_key[i]].values()]    # subcategory(num) list
            for k in range(len(subkey_list)):
                subkey = subkey_list[k]    # subcategory(kor)
                subvalue_dict = subvalue_list[k]   # subcategory(num)
                url_list = 'https://www.segye.com/boxTemplate/newsList/box/newsList.do'
                param = {'dataPath': f'{cate_values[i]}{subvalue_dict}',
                         'dataId': f'{cate_values[i]}{subvalue_dict}',
                         'listSize': '10',
                         'naviSize': '10',
                         'dataType': 'list'}
                self.LOGGER.info(url_list)
                try:
                    res = requests.post(url_list, headers=self.header, params=param)
                    soup = BeautifulSoup(res.content, 'lxml')
                    temp = soup.find('ul', attrs={'class': 'listBox'})
                    temp_list = temp.find_all('li')
                    self.insert_mongo(temp_list, cate_key[i], cate_values[i], subkey_list[k], subvalue_list[k])
                except Exception as ee:
                    self.LOGGER.error(ee)
        return i

    def crawler(self, news, cate_key, cate_values, subkey_list, subvalue_list) -> dict:
        segye_list = {}
        self.LOGGER.info(f"segye {cate_key}, {subkey_list} started...")
        href = news.find('a').get('href')
        aid = href.split('/')[-1]
        segye_list['oid'] = '022'   # segye oid
        segye_list['_id'] = '022' + aid
        segye_list['sid'] = cate_values
        segye_list['sid2'] = subvalue_list
        segye_list['aid'] = aid
        segye_list['headline'] = news.find('strong', attrs={'class': 'tit'}).get_text().strip()
        all_content = news.find('span', attrs={'class': 'cont'}).get_text().strip()
        segye_list['summary'] = textwrap.shorten(all_content, width=100, placeholder=' ...')
        segye_list['category'] = cate_key
        segye_list['subcategory'] = subkey_list
        segye_list['press'] = '세계일보'
        try:
            segye_list['img_url'] = news.find('img').get('src')
        except:
            segye_list['img_url'] = None
        segye_list['content_url'] = 'https://www.segye.com' + href
        segye_list['naver_link'] = segye_list['content_url'] + '?OutUrl=naver'
        sleep(3)

        ## body_raw
        body_res = requests.get(segye_list['content_url'], headers=self.header)
        body_soup = BeautifulSoup(body_res.content, 'lxml')
        segye_list['body_raw'] = f"{body_soup}"

        ## 기자 이름
        try:
            reporter = body_soup.find('article', attrs={'class': 'viewBox2'}).find('div').get_text()
            # regex = "\(.*\)|\s\s.*"
            # reporter = re.sub(regex, '', reporter).strip()
            email_pattern = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9_.]+)'
            reporter = re.sub(pattern=email_pattern, repl='', string=reporter).strip()
            segye_list['author'] = reporter
        except:
            segye_list['author'] = None

        try:
            date = body_soup.find('p', attrs={'class': 'viewInfo'}).get_text()
            date = date.split(' : ')[1]
            date = date.replace('수정', '').strip()
            segye_list['article_date'] = dt.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y.%m.%d %H:%M')
        except:
            date = body_soup.find('p', attrs={'class': 'viewInfo'}).get_text()
            date = date.split(' : ')[-1]
            segye_list['article_date'] = dt.datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y.%m.%d %H:%M')

        try:
            edit_date = body_soup.find('span', attrs={'class': 'modify'}).get_text()
            edit_date = edit_date.split(' : ')[-1]
            segye_list['article_editdate'] = dt.datetime.strptime(edit_date, '%Y-%m-%d %H:%M:%S').strftime('%Y.%m.%d %H:%M')
        except:
            segye_list['article_editdate'] = None

        segye_list['news_date'] = dt.datetime.strptime(segye_list['article_date'], '%Y.%m.%d %H:%M').strftime('%Y%m%d')
        segye_list['insertTime'] = str(datetime.now())
        return segye_list

    def mongo_data(self, segye_list) -> dict:
        mongo_data = {'_id': segye_list['_id'],
                      'sid': segye_list['sid'],
                      'sid2': segye_list['sid2'],
                      'oid': segye_list['oid'],
                      'aid': segye_list['aid'],
                      'news_date': segye_list['news_date'],
                      'category': segye_list['category'],
                      'subcategory': segye_list['subcategory'],
                      'press': segye_list['press'],
                      'headline': segye_list['headline'],
                      'summary': segye_list['summary'],
                      'body_raw': segye_list['body_raw'],
                      'article_date': segye_list['article_date'],
                      'article_editdate': segye_list['article_editdate'],
                      'author': segye_list['author'],
                      'content_url': segye_list['content_url'],
                      'naver_link' : segye_list['naver_link'],
                      'img_url': segye_list['img_url'],
                      'import_result': 'N',
                      'search_result': 'N',
                      'insertTime': segye_list['insertTime'],
                      }
        return mongo_data

    def insert_mongo(self, all_news, cate_key, cate_values, subkey_list, subvalue_list):
        count = 0
        for news in all_news:
            segye_list = self.crawler(news, cate_key, cate_values, subkey_list, subvalue_list)
            mongo_data = self.mongo_data(segye_list)
            ## 중복 처리
            try:
                # print(mongo_data)
                self.db_connection.mongodb_connection(mongo_data, self.conn)
                # self.socket.send_pyobj(mongo_data)
                self.LOGGER.info('segye_news Mongo Insert : ' + mongo_data['_id'])
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
    start = Segye()
    start.url_list()


if __name__ == '__main__':
    me = singleton.SingleInstance()

    times = datetime.today().strftime("%Y%m%d")
    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'
    loguru.logger.add(
        sink=f'./logs/segye_Main_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    main()