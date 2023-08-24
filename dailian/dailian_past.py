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
from db_mongo.past_mongo import PastMongoDB


class PastDailian:
    """ 데일리안 뉴스 크롤러 """

    def __init__(self):
        self.header = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
        self.db_connection = PastMongoDB()
        self.conn = self.db_connection.mongodb_primary()
        self.LOGGER = loguru.logger.bind(name='service')
        self.cate_list = self.category_list()[0]
        self.sub_list = self.category_list()[1]
        self.socket = self.zmq_connect()

    def zmq_connect(self):
        context = zmq.Context()
        perser = context.socket(zmq.PUSH)
        perser.connect("tcp://127.0.0.1:5119")
        return perser

    def category_list(self) -> dict:
        category_list = {'정치':'politics', '사회':'society', '수도권':'capital', '경제':'economy', 'IT/과학':'itScience',
                         '연예':'entertainment', '스포츠':'sports', '생활문화':'lifeCulture', '세계':'world'}
        subcategory_list = {}
        subcategory_list['politics'] = {'정치일반':'201', '대통령실':'202', '국회':'203', '정당':'204', '북한':'205', '국방·외교':'206'}
        subcategory_list['society'] = {'사회일반':'301', '사건사고':'302', '교육':'303', '노동':'304', '언론':'305', '환경':'306', '인권·복지':'307', '식품·의료':'308', '지역':'309', '인물':'310'}
        subcategory_list['capital'] = {'경기남부':'1301', '경기북부':'1302', '인천':'1303'}
        subcategory_list['economy'] = {'경제일반':'401', '금융':'402', '증권':'403', '산업':'404', '재계':'405', '중기':'406', '벤처':'407', '부동산':'408', '글로벌경제':'409', '생활경제':'410'}
        subcategory_list['itScience'] = {'IT일반':'501', '과학일반':'502', '모바일':'503', '인터넷·SNS':'504', '통신':'505', '보안·해킹':'506', '컴퓨터':'507', '게임':'508', '리뷰':'509'}
        subcategory_list['entertainment'] = {'연예일반':'801', 'TV':'802', '영화':'803', '음악':'804', '스타':'805'}
        subcategory_list['sports'] = {'스포츠일반':'901', '축구':'902', '해외축구':'903', '야구':'904', '해외야구':'905', '농구':'906', '배구':'907', 'UFC':'908', '골프':'909'}
        subcategory_list['lifeCulture'] = {'생활문화일반':'601', '건강정보':'602', '자동차·시승기':'603', '도로·교통':'604', '여행·레저':'605', '음식·맛집':'606', '패션·뷰티':'607', '공연·전시':'608', '책':'609', '종교':'610', '날씨':'611'}
        subcategory_list['world'] = {'세계일반':'701', '아시아·오세아니아':'702', '북미':'703', '중남미':'704', '유럽':'705', '중동·아프리카':'706'}

        return category_list, subcategory_list

    def url_list(self):
        cate_key = [i for i in self.cate_list.keys()]  # category(kor)
        cate_values = [i for i in self.cate_list.values()]  # category(eng)
        for i in range(len(cate_values)):
            sub_key = [j for j in self.sub_list[cate_values[i]]]  # subcategory(kor) list
            sub_values = [j for j in self.sub_list[cate_values[i]].values()]    # subcategory(num) list
            for k in range(len(sub_key)):
                subkey_list = sub_key[k]    # subcategory(kor)
                subvalue_list = sub_values[k]   # subcategory(num)
                for page in range(1, 10000):
                    url_list = f'https://dailian.co.kr/newslist?category2={subvalue_list}&page={page}'
                    self.LOGGER.info(url_list)
                    try:
                        res = requests.get(url_list, headers=self.header)
                        soup = BeautifulSoup(res.content, 'lxml')
                        temp = soup.find('div', attrs={'id': 'contentsArea'})
                        temp_list = temp.find_all('div', attrs={'class': 'vCenter flex'})
                        article = temp.find('div', attrs={'class':'wide1Box'})
                        if article is None:
                            break
                        else:
                            self.insert_mongo(temp_list, cate_key[i], cate_values[i], subkey_list, subvalue_list)
                    except Exception as ee:
                        self.LOGGER.error(ee)
        return i

    def crawler(self, news, cate_key, cate_values, subkey_list, subval_list) -> dict:
        dailian_list = {}
        self.LOGGER.info(f"dailian {cate_key}, {subkey_list} started...")
        href = news.find('a').get('href')
        aid = href.split('/')[-2]
        dailian_list['oid'] = '119'   # dailian oid
        dailian_list['_id'] = '119' + aid
        dailian_list['sid'] = cate_values
        dailian_list['sid2'] = subval_list
        dailian_list['aid'] = aid
        dailian_list['headline'] = news.find('p', attrs={'class': 'marginTop5 subtitle3 font500 lineLimit line1'}).text.strip()
        dailian_list['summary'] = news.find('p', attrs={'class': 'marginTop10 itemSubtitle lineLimit line3'}).text.strip()
        dailian_list['category'] = cate_key
        dailian_list['subcategory'] = subkey_list
        dailian_list['press'] = '데일리안'
        try:
            dailian_list['img_url'] = news.find('img').get('src')
        except:
            dailian_list['img_url'] = None
        dailian_list['content_url'] = 'https://dailian.co.kr' + href
        dailian_list['naver_link'] = dailian_list['content_url'] + '?sc=Naver'
        sleep(3)

        ## body_raw
        body_res = requests.get(dailian_list['content_url'], headers=self.header)
        body_soup = BeautifulSoup(body_res.content, 'lxml')
        dailian_list['body_raw'] = f"{body_soup}"

        ## 기자 이름
        reporter = body_soup.find('div', attrs={'class': 'reportText'}).get_text().split('(')[0].strip()
        try:
            dailian_list['author'] = reporter
        except:
            dailian_list['author'] = None

        date_json = body_soup.find('script', attrs={'type':'application/ld+json'})
        date_list = json.loads(date_json.contents[0])
        article_date = re.sub('T', ' ', date_list['datePublished'].split('+')[0])
        dailian_list['article_date'] = dt.datetime.strptime(article_date, '%Y-%m-%d %H:%M:%S').strftime('%Y.%m.%d %H:%M')
        try:
            article_editdate = re.sub('T', ' ', date_list['dateModified'].split('+')[0])
            dailian_list['article_editdate'] = dt.datetime.strptime(article_editdate, '%Y-%m-%d %H:%M:%S').strftime('%Y.%m.%d %H:%M')
        except:
            dailian_list['article_editdate'] = None
        dailian_list['news_date'] = dt.datetime.strptime(dailian_list['article_date'], '%Y.%m.%d %H:%M').strftime('%Y%m%d')
        dailian_list['insertTime'] = str(datetime.now())
        return dailian_list

    def mongo_data(self, dailian_list) -> dict:
        mongo_data = {'_id': dailian_list['_id'],
                      'sid': dailian_list['sid'],
                      'sid2': dailian_list['sid2'],
                      'oid': dailian_list['oid'],
                      'aid': dailian_list['aid'],
                      'news_date': dailian_list['news_date'],
                      'category': dailian_list['category'],
                      'subcategory': dailian_list['subcategory'],
                      'press': dailian_list['press'],
                      'headline': dailian_list['headline'],
                      'summary': dailian_list['summary'],
                      'body_raw': dailian_list['body_raw'],
                      'article_date': dailian_list['article_date'],
                      'article_editdate': dailian_list['article_editdate'],
                      'author': dailian_list['author'],
                      'content_url': dailian_list['content_url'],
                      'naver_link' : dailian_list['naver_link'],
                      'img_url': dailian_list['img_url'],
                      'import_result': 'N',
                      'search_result': 'N',
                      'insertTime': dailian_list['insertTime'],
                      }
        return mongo_data

    def insert_mongo(self, all_news, cate_key, cate_values, subkey_list, subval_list):
        count = 0
        for news in all_news:
            dailian_list = self.crawler(news,cate_key, cate_values, subkey_list, subval_list)
            mongo_data = self.mongo_data(dailian_list)
            ## 중복 처리
            try:
                # print(mongo_data)
                self.db_connection.mongodb_connection(mongo_data, self.conn)
                # self.socket.send_pyobj(mongo_data)
                self.LOGGER.info('dailian_news Mongo Insert : ' + mongo_data['_id'])
                sleep(3)
            except Exception as ee:
                self.LOGGER.error(f'data duplicated : {mongo_data["_id"]}, {mongo_data["article_date"]}')
                count += 1
                print('count : ', count)
                if count == 3:
                    return news


def main():
    start = PastDailian()
    start.url_list()


if __name__ == '__main__':
    me = singleton.SingleInstance()

    times = datetime.today().strftime("%Y%m%d")
    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'
    loguru.logger.add(
        sink=f'./logs/dailian_Main_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    main()