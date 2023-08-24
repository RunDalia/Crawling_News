# -*- coding: utf-8 -*-
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


class Wowtv:
    """ 한국 경제 TV 뉴스 크롤러 """

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
        parser = context.socket(zmq.PUSH)
        parser.connect("tcp://127.0.0.1:5215")
        return parser

    def category_list(self) -> dict:
        '''
        한국경제TV의 모든 카테고리 정보를 결과 값으로 반환해주는 함수입니다.

        :return:
            category_list : 한국경제TV의 모든 카테고리 dictionary
            category_url_list : 각 카테고리의 url을 값으로 갖는 dictionary
        '''
        category_list = {'economy': '경제', 'stock': '증권', 'init': '산업IT', 'virtualcurrency': '가상화폐',
                         'politics': '정치사회', 'international': '국제', 'sports': '스포츠', 'entstar': '연예'}
        subcategory_list = {'경제': '529', '증권': '460', '산업IT': '902', '가상화폐': '21138',
                            '정치사회': '79252', '국제': '587', '스포츠': '79073', '연예': '79072'}

        return category_list, subcategory_list

    def url_list(self):
        '''
        각 카테고리의 모든 뉴스 기사 리스트를 불러 와서
        insert_mongo() 함수에 기사 내용을 넣는 함수입니다.
        '''
        cate_key = [i for i in self.cate_list.keys()]  # category(eng)
        cate_values = [i for i in self.cate_list.values()]  # category(kor)
        for i in range(len(cate_values)):
            sub_key = [i for i in self.sub_list.keys()]  # subcategory(kor) list
            sub_values = [i for i in self.sub_list.values()]  # subcategory(num) list
            url = f'https://www.wowtv.co.kr/NewsCenter/News/NewsList?subMenu={cate_key[i]}&menuSeq={sub_values[i]}'
            only_wowtv = '&searchComp=WO'
            wowtv_url = url + only_wowtv
            self.LOGGER.info(wowtv_url)
            try:
                res = requests.get(wowtv_url, headers=self.header)
                soup = BeautifulSoup(res.content, 'lxml')
                soup2 = soup.find('div', attrs={'class': 'contain-list-news'})
                all_news = soup2.find_all('div', attrs={'class': 'article-news-list'})
                self.insert_mongo(all_news, cate_key[i], cate_values[i], sub_values[i])
            except Exception as ee:
                self.LOGGER.error(ee)
        return i

    def crawler(self, news, cate_key, cate_values, sub_values) -> dict:
        '''
        뉴스를 하나씩 받아와서 모든 정보를 파싱하여 dictionary에 저장하여 반환하는 함수입니다.

        :param news: 하나의 뉴스 기사
        :param c_key: 카테고리(ENG)
        :param c_values: 카테고리(KOR)
        :return: wowtv_list : 하나의 기사의 모든 정보를 담고 있는 dictionary
        '''
        wowtv_list = {}
        self.LOGGER.info(f"wowtv {cate_key}, {cate_values} started...")
        href = news.find('a').get('href')
        aid = href.split('=')[1]
        wowtv_list['oid'] = '215'
        wowtv_list['_id'] = '215' + aid
        wowtv_list['sid'] = cate_key
        wowtv_list['sid2'] = sub_values
        wowtv_list['aid'] = aid
        headline = news.find('p', attrs={'class': 'title-text'}).text
        wowtv_list['headline'] = re.sub(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', '', headline).strip()
        wowtv_list['summary'] = news.find('p', attrs={'class': 'main-text'}).text.strip()
        wowtv_list['category'] = cate_values
        wowtv_list['subcategory'] = None
        wowtv_list['press'] = '한국경제TV'
        try:
            wowtv_list['img_url'] = news.find('img').get('src')
        except:
            wowtv_list['img_url'] = None
        wowtv_list['content_url'] = href
        wowtv_list['naver_link'] = href + '&t=NN'
        sleep(3)

        ## body_raw
        body_res = requests.get(wowtv_list['content_url'], headers=self.header)
        body_soup = BeautifulSoup(body_res.content, 'lxml')
        wowtv_list['body_raw'] = f"{body_soup}"

        ## 기자 이름
        try:
            reporter = body_soup.find('div', attrs={'class': 'info-reporter'}).get_text()
            regex = "\(.*\)|\s\s.*"
            email_pattern = '([a-zA-Z0-9_.+-]+@[a-zA-Z0-9_.]+)'
            reporter = re.sub(regex, '', reporter).strip()
            reporter = re.sub(pattern=email_pattern, repl='', string=reporter).strip()
            author = reporter.split()[-2:]
            author = " ".join(str(s) for s in author)
            wowtv_list['author'] = author
        except:
            wowtv_list['author'] = None

        date_info = body_soup.find('p', attrs={'class': 'date-news'})
        article_date = date_info.select('span', attrs={'class': 'text-number'})[0].text
        wowtv_list['article_date'] = dt.datetime.strptime(article_date, '%Y-%m-%d %H:%M').strftime('%Y.%m.%d %H:%M')
        try:
            article_editdate = date_info.select('span', attrs={'class': 'text-number'})[1].text
            wowtv_list['article_editdate'] = dt.datetime.strptime(article_editdate, '%Y-%m-%d %H:%M').strftime(
                '%Y.%m.%d %H:%M')
        except:
            wowtv_list['article_editdate'] = None
        wowtv_list['news_date'] = dt.datetime.strptime(wowtv_list['article_date'], '%Y.%m.%d %H:%M').strftime('%Y%m%d')
        wowtv_list['insertTime'] = str(datetime.now())
        return wowtv_list

    def mongo_data(self, wowtv_list) -> dict:
        '''
        DB에 데이터를 삽입하기 위해 기존 포맷에 맞춰 dictionary 재정의

        :param wowtv_list: 하나의 기사의 모든 정보를 담고 있는 dictionary
        :return: DB 포맷에 맞춘 dictionary
        '''
        mongo_data = {'_id': wowtv_list['_id'],
                      'sid': wowtv_list['sid'],
                      'sid2': wowtv_list['sid2'],
                      'oid': wowtv_list['oid'],
                      'aid': wowtv_list['aid'],
                      'news_date': wowtv_list['news_date'],
                      'category': wowtv_list['category'],
                      'subcategory': wowtv_list['subcategory'],
                      'press': wowtv_list['press'],
                      'headline': wowtv_list['headline'],
                      'summary': wowtv_list['summary'],
                      'body_raw': wowtv_list['body_raw'],
                      'article_date': wowtv_list['article_date'],
                      'article_editdate': wowtv_list['article_editdate'],
                      'author': wowtv_list['author'],
                      'content_url': wowtv_list['content_url'],
                      'naver_link': wowtv_list['naver_link'],
                      'img_url': wowtv_list['img_url'],
                      'import_result': 'N',
                      'search_result': 'N',
                      'insertTime': wowtv_list['insertTime'],
                      }
        return mongo_data

    def insert_mongo(self, all_news, cate_key, cate_values, sub_values):
        '''
        하나의 카테고리의 모든 기사 리스트를 받아와서 crawler 함수에 전달하고 return으로 받아온 값을
        mongo_data 함수에 전달하여 기사 하나의 모든 정보를 하나의 dictionary에 저장하여
        Queue에 저장 후 중복 처리하여 중북 데이터가 아닌 경우 DB에 삽입하는 함수 입니다.

        :param temp_main: 하나의 카테고리에 속한 기사들
        :param c_key: 카테고리(ENG)
        :param c_values: 카테고리(KOR)
        :return:
        '''
        count = 0
        for news in all_news:
            wowtv_list = self.crawler(news, cate_key, cate_values, sub_values)
            mongo_data = self.mongo_data(wowtv_list)
            ## 중복 처리
            try:
                # print(mongo_data)
                self.db_connection.mongodb_connection(mongo_data, self.conn)
                self.socket.send_pyobj(mongo_data)
                self.LOGGER.info('wowtv_news Mongo Insert : ' + mongo_data['_id'])
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
    start = Wowtv()
    start.url_list()


if __name__ == '__main__':
    me = singleton.SingleInstance()

    times = datetime.today().strftime("%Y%m%d")
    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'
    loguru.logger.add(
        sink=f'./logs/wowtv_Main_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    main()