# -*- coding: utf-8 -*-
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz


def main(args):
    # ========키워드 인자(argument,arg) 가져오기 ========
    db_info = args.get('DB')
    before_date = args.get('BEFORE_DATE')

    # ========DB 접근설정 ========
    client = MongoClient(host=db_info['my_ip'], port=27017,
                         username=db_info['username'], password=db_info['password'])
    db = client[db_info['db_name']]
    collection = db[db_info['collection_name']]
    collection_new = db[db_info['collection_name_new']]

    # =====Delete Date field to DB if null=======
    collection.delete_many({'sentiment': "null"})

    # =====Read from Date=======
    target_date = cal_datetime(before_date)
    sentiment_items = list(collection.find(
        {'date': {'$gte': target_date['date_st'], '$lte': target_date['date_end']}}, {'_id': False}))

    neutral = 0
    positive = 0
    negative = 0

    for item in sentiment_items:
        # IndexError 예외처리
        try:
            sentiment = item['sentiment'][0]['sentiment']
            if (sentiment == 'neutral'):
                neutral += 1
            elif (sentiment == 'positive'):
                positive += 1
            elif (sentiment == 'negative'):
                negative += 1
        except IndexError: 
            print('IndexError 에러 발생, exception처리완료')    
        

    sentiment = [neutral, positive, negative]
    max_sentiment = max(sentiment)

    if (neutral == max_sentiment and positive == max_sentiment and negative == max_sentiment):
        daily_sentiment = 'neutral'
    elif (neutral == max_sentiment and positive == max_sentiment):
        daily_sentiment = 'positive'
    elif (neutral == max_sentiment and negative == max_sentiment):
        daily_sentiment = 'negative'
    elif (positive == max_sentiment and negative == max_sentiment):
        daily_sentiment = 'neutral'
    elif (neutral == max_sentiment):
        daily_sentiment = 'neutral'
    elif (negative == max_sentiment):
        daily_sentiment = 'negative'
    elif (positive == max_sentiment):
        daily_sentiment = 'positive'

    sentiment = {
        # ## sentiment 날짜포맷 변경
        # 'date': datetime.now(),
        'date': target_date['date_end'].strftime('%Y-%m-%d'),
        'neutral': neutral,
        'positive': positive,
        'negative': negative,
        'dailySentiment': daily_sentiment
    }

    # print(sentiment)

    # ## sentiment 날짜포맷 변경 및 재수행시, 기존 sentiment 데이터 삭제후 재조회 입력
    collection_new.delete_one({'date': target_date['date_end'].strftime('%Y-%m-%d')})
    collection_new.insert_one(sentiment)

    return {'process': 'end'}

# =====Date function=======


def cal_datetime(before_date, timezone='Asia/Seoul'):
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
    end = target_date.replace(
        hour=23, minute=59, second=59, microsecond=999999).astimezone(pytz.UTC)

    return {'date_st': start, 'date_end': end}
