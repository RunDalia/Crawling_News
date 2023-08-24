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


class Kmib:
    def __init__(self):
        self.harder = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
        self.db_connetion = MongoDB()
        self.conn = self.db_connetion.mongodb_primary()
        self.LOGGER = loguru.logger.bind(name='service')
        self.cate_list = self.category_list()[0]
        self.cate_url = self.category_list()[1]
        self.socket = self.zmq_connct()

    def zmq_connct(self):
        context = zmq.Context()
        perser = context.socket(zmq.PUSH)
        perser.connect("tcp://127.0.0.1:5003")
        return perser

    def category_list(self):
        category_list = {'pol':'정치', 'eco':'경제', 'soc':'사회', 'int':'국제', 'spo':'스포츠', 'lif':'라이프'}
        category_url_list = {}
        category_url_list['pol'] = {'https://news.kmib.co.kr/article/list.asp?sid1=pol'}
        category_url_list['eco'] = {'https://news.kmib.co.kr/article/list.asp?sid1=eco'}
        category_url_list['soc'] = {'https://news.kmib.co.kr/article/list.asp?sid1=soc'}
        category_url_list['int'] = {'https://news.kmib.co.kr/article/list.asp?sid1=int'}
        category_url_list['spo'] = {'https://news.kmib.co.kr/article/list.asp?sid1=spo'}
        category_url_list['lif'] = {'https://news.kmib.co.kr/article/list.asp?sid1=lif'}
        return category_list, category_url_list

    def url_list(self):
        c_key = [i for i in self.cate_list.keys()]   # category(eng)
        c_values = [i for i in self.cate_list.values()]  # category(kor)
        for i in range(len(c_key)):
            url_list = [j for j in self.cate_url[c_key[i]]][0]  # category 별 url
            self.LOGGER.info(url_list)
            try:
                res = requests.get(url_list, headers=self.harder)
                soup = BeautifulSoup(res.content,'lxml')
                soup2 = soup.find('div',attrs={'id':'nws_list'})     # 해당 페이지 모든 뉴스 기사 목록
                temp_main = soup2.find_all('li',attrs={'class':'nws'})   # 기사 하나에 대한 내용(이미지, 제목, 본문(요약) 등...)
                self.insert_mongo(temp_main, c_key[i], c_values[i])
            except Exception as ee:
                self.LOGGER.error(ee)
        return i


    def crawler(self, news, c_key, c_values):
        kmib_list = {}
        self.LOGGER.info(f"mt {c_key}, {c_values} started...")
        href = news.find('a').get('href')
        href_aid = href.split('&')[0]
        aid = re.sub(r'[^0-9]','',href_aid)
        kmib_list['oid'] = '005'
        kmib_list['_id'] = kmib_list['oid'] + aid
        kmib_list['sid'] = c_key
        kmib_list['sid2'] = None
        kmib_list['aid'] = aid
        kmib_list['headline'] = news.find('dt').text.strip()
        summary = news.find('dd',attrs={'class':'tx'})
        kmib_list['summary'] = summary.find('a').text.strip()
        kmib_list['category'] = c_values
        kmib_list['subcategory'] = None
        kmib_list['press'] = '국민일보'
        try:
            kmib_list['img_url'] = news.find('img').get('src')
        except:
            kmib_list['img_url'] = None
        kmib_list['content_url'] = href
        try :
            n_rn = news.find('span', attrs={'class':'etc'}).text
            kmib_list['author'] = re.sub(r'[^가-힇]','',n_rn).replace('@','').strip()
        except:
            kmib_list['author'] = None
        sleep(3)
        ## body_raw
        body_res = requests.get(kmib_list['content_url'], headers=self.harder)
        body_soup = BeautifulSoup(body_res.content,'lxml')
        kmib_list['body_raw'] = f"{body_soup}"
        times_is = body_soup.select('span[class=t11]')
        article_date = times_is[0]
        kmib_list['article_date'] =str(article_date).split('>')[1].split('<')[0].replace('-', '.')
        try:
            article_editdate = times_is[1]
            kmib_list['article_editdate'] = str(article_editdate).split('>')[1].split('<')[0].replace('-', '.')
        except:
            article_editdate = None
            kmib_list['article_editdate'] = None

        kmib_list['news_date'] = dt.datetime.strptime(kmib_list['article_date'],'%Y.%m.%d %H:%M').strftime('%Y%m%d')
        kmib_list['insertTime'] = str(datetime.now())
        return kmib_list

    def mongo_data(self, kmib_list):
        mongo_data = {'_id': kmib_list['_id'],
                      'sid': kmib_list['sid'],
                      'sid2': kmib_list['sid2'],
                      'oid': kmib_list['oid'],
                      'aid': kmib_list['aid'],
                      'news_date': kmib_list['news_date'],
                      'category': kmib_list['category'],
                      'subcategory': kmib_list['subcategory'],
                      'press': kmib_list['press'],
                      'headline': kmib_list['headline'],
                      'summary': kmib_list['summary'],
                      'body_raw': kmib_list['body_raw'],
                      'article_date': kmib_list['article_date'],
                      'article_editdate': kmib_list['article_editdate'],
                      'author': kmib_list['author'],
                      'content_url': kmib_list['content_url'],
                      'img_url': kmib_list['img_url'],
                      'insertTime': kmib_list['insertTime'],
                      }
        return mongo_data

    def insert_mongo(self, temp_main, c_key, c_values):
        count = 0
        for news in temp_main:
            kmib_list = self.crawler(news, c_key, c_values)
            mongo_data = self.mongo_data(kmib_list)
            ## 중복 처리
            try:
                # print(mongo_data)
                self.db_connetion.mongodb_connaction(mongo_data, self.conn)
                # self.socket.send_pyobj(mongo_data)
                self.LOGGER.info('mt_news Mongo Insert : ' + mongo_data['_id'])
                sleep(3)
            except Exception as ee:
                self.LOGGER.error(ee)
                count += 1
                print('count : ', count)
                if count == 3:
                    return news


def main():
    start = Kmib()
    start.url_list()


if __name__ == '__main__':
    me = singleton.SingleInstance()

    times = datetime.today().strftime("%Y%m%d")
    log_format = '[{time:YYYY-MM-DD HH:mm:ss.SSS}] [{process: >5}] [{level.name:>5}] <level>{message}</level>'
    loguru.logger.add(
        sink=f'./logs/mt_Main_{times}.log',
        format=log_format,
        enqueue=True,
        level='INFO'.upper(),
        rotation='10 MB',
    )
    main()