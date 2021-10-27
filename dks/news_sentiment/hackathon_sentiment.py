# -*- coding: utf-8 -*-
import requests
import json
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz


def main(args):
    """
    CLOVA SUMMARY API 사용해서 뉴스 DB에 요약정보 업데이트하기
    """

    # ========키워드 인자(argument,arg) 가져오기 ========
    db_info = args.get('DB')
    before_date = args.get('BEFORE_DATE')
    sentiment_api_info = args.get('SENTIMENT_API')

    # ========DB 접근설정 ========
    client = MongoClient(host=db_info['my_ip'], port=27017,
                         username=db_info['username'], password=db_info['password'])
    db = client[db_info['db_name']]
    collection = db[db_info['collection_name']]

    # =====Read from Date=======
    target_date = cal_datetime_utc(before_date)

    sentiment_items = list(collection.find(
        {'sentiment': {'$exists': False}, 'date': {'$gte': target_date['date_st'], '$lte': target_date['date_end']}}, {'_id': False}))

    # =====Sentiment=======
    for item in sentiment_items:
        if 10 < len(item['summary']) < 1000:
            result = sentiment(txt=item['summary'], client_id=sentiment_api_info['client_id'],
                               client_secret=sentiment_api_info['client_secret'])
        else:
            result = 'null'

        # DB 에 업데이트
        collection.update_one(
            {'link': item['link']}, {'$set': {'sentiment': result}})

    return {'process': 'end'}


def cal_datetime_utc(before_date, timezone='Asia/Seoul'):
    '''
    현재 일자에서 before_date 만큼 이전의 일자를 UTC 시간으로 변환하여 반환
    :param before_date: 이전일자
    :param timezone: 타임존
    :return: UTC 해당일의 시작시간(date_st)과 끝 시간(date_end)
    :rtype: dict of datetime object
    :Example:
    2021-09-13 KST 에 get_date(1) 실행시,
    return은 {'date_st': datetype object 형태의 '2021-09-11 15:00:00+00:00'), 'date_end': datetype object 형태의 '2021-09-12 14:59:59.999999+00:00'}
    '''
    today = pytz.timezone(timezone).localize(datetime.now())
    target_date = today - timedelta(days=before_date)

    # 같은 일자 same date 의 00:00:00 로 변경 후, UTC 시간으로 바꿈
    start = target_date.replace(hour=0, minute=0, second=0,
                                microsecond=0).astimezone(pytz.UTC)

    # 같은 일자 same date 의 23:59:59 로 변경 후, UTC 시간으로 바꿈
    end = target_date.replace(hour=23, minute=59, second=59, microsecond=999999).astimezone(pytz.UTC)

    return {'date_st': start, 'date_end': end}


def sentiment(txt, client_id, client_secret):
    '''
    텍스트 입력받아서 요약 - clova sentiment api
    :param: text 요약할 텍스트
    :param: client_id api 사용시 필요한 client_id
    :param: client_secret api 사용시 필요한 client_secret
    :return: summary_txt 요약된 텍스트
    :rtype: str
    '''

    headers = {'X-NCP-APIGW-API-KEY-ID': client_id,
               'X-NCP-APIGW-API-KEY': client_secret,
               'Content-Type': 'application/json'}

    data = {'content': txt}

    r = requests.post('https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze',
                        headers=headers, data=json.dumps(data))

    sentiment_items = []

    if r.status_code == requests.codes.ok:
        result_response = json.loads(r.content)

        sense = {
            'sentiment': result_response['document']['sentiment'],
            'neutral': round(result_response['document']['confidence']['neutral'], 3),
            'positive': round(result_response['document']['confidence']['positive'], 3),
            'negative': round(result_response['document']['confidence']['negative'], 3),
        }
        sentiment_items.append(sense)

    else:
        print('request failed')
        failed_msg = json.loads(r.content.decode('utf-8'))
        print(failed_msg)

    return sentiment_items