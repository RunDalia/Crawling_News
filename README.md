# 국내 언론사 데이터 수집용 Crawling Code 개발
***
인공지능 기반의 뉴스 분석 주식정보를 제공하는 서비스를 운영하기 위한 뉴스 기사 수집 프로젝트입니다. 매일 실시간으로 AI모델이 분석한 뉴스를 종목 배정하여, 소비자들이 검색 없이 원하는 종목 정보를 한 눈에 확인할 수 있는 서비스를 제공받게 하고자 시작되었습니다.
   
### 수집 언론사 
| 폴더명 | 언론사 |
|----|----|
| dailian | 데일리안 |
| etnews | 매일경제 |
| kmib | 국민일보 |
| segye | 세계일보 |
| wowtv | 한국경제 TV |

### 구성 요소 
###### config
DB 정보가 포함되어 있어 GitHub에는 업로드하지 않아 빈 폴더입니다. 

###### db_mongo
- mongo.py : 정해진 DB에 연결하는 코드가 들어있습니다.
- past_mongo.py : 과거의 데이터를 정해진 DB에 연결하는 코드가 들어있습니다.

###### logs
실제 서버 패치 후 수집된 log를 모아둔 폴더입니다. 

###### parser
수집된 데이터 중에서 뉴스 기사 내용만 전처리하는 코드가 들어있습니다. 

###### main\_start.py
해당 언론사의 모든 데이터를 수집하는 코드입니다. 뉴스 제목, 본문, 기자, 날짜, url 등 가능한 모든 정보를 수집했습니다. 

###### 언론사\_past.py
과거의 뉴스를 개발하는 코드입니다. main\_start.py는 실행 이후에 발행되는 기사를 수집하는 코드이기 때문에, 과거의 뉴스를 수집할 수 있는 코드 또한 개발했습니다.
