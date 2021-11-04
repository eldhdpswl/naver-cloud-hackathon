from flask import Flask, render_template, jsonify
from pymongo import MongoClient
import boto3
from datetime import datetime, timedelta
import pytz
import json


app = Flask(__name__)

# ======Flask 에 필요한 설정 정보 config.json 에서 가져오기======
with open('config.json', 'r') as f:
    config = json.load(f)
db_info = config['DB']


@app.route("/")
def home():
    """
    화면에 접속
    """
    return render_template("index.html")


@app.route("/api/news")
def send_news():
    """
    전날 뉴스 데이터 가져오기
    """
    client = MongoClient(host=db_info['my_ip'], port=27017,
                         username=db_info['username'], password=db_info['password'])
    db = client[db_info['db_name']]
    collection = db[db_info['collection_name']]

    # target_date = cal_datetime_utc(2)
    # # print(f'UTC targetdate: {target_date}')

    # ======before_date 일 전 데이터 조회하기=======
    target_date = cal_datetime_utc(0)
    # print(f'UTC targetdate: {target_date}')

    news_items = list(collection.find(
        {'date': {'$gte': target_date['date_st'], '$lte': target_date['date_end']}}, {'_id': False}).sort('date', 1).limit(12))
    # print(news_items)

    return jsonify({"news": news_items})


@app.route("/sentiments")
def send_sentiments():

    client = MongoClient(host=db_info['my_ip'], port=27017,
                         username=db_info['username'], password=db_info['password'])
    db = client[db_info['db_name']]
    collection = db[db_info['collection_name_daily']]

    datas = list(collection.find())
    # print(news_items)

    sentiment_datas = []
    for data in datas:
        date = data['date']
        if (date.day < 10):
            date = f'{date.year}-{date.month}-0{date.day}'
        else:
            date = f'{date.year}-{date.month}-{date.day}'
        sentiment = data['dailySentiment']
        color = ''
        if sentiment == 'positive':
            color = '#799fcb'
        elif sentiment == 'neutral':
            color = '#9bd0b7'
        else:
            color = '#f9665e'

        sentiment_data = {'title': sentiment, 'start': date,
                          'color': color}
        sentiment_datas.append(sentiment_data)

    return jsonify({"sentiments": sentiment_datas})


@app.route("/sentiments/today")
def send_sentiments_today():

    client = MongoClient(host=db_info['my_ip'], port=27017,
                         username=db_info['username'], password=db_info['password'])
    db = client[db_info['db_name']]
    collection = db[db_info['collection_name_daily']]

    # ======before_date 일 전 데이터 조회하기=======
    target_date = cal_datetime_utc(0)
    # print(f'UTC targetdate: {target_date}')

    datas = list(collection.find(
        {'date': {'$gte': target_date['date_st'], '$lte': target_date['date_end']}}, {'_id': False}).sort('date', 1).limit(12))
    sentiment_datas = []
    for data in datas:
        date = data['date']
        date = f'{date.year}-{date.month}-{date.day}'
        sentiment = data['dailySentiment']

        sentiment_data = {'today': sentiment, 'date': date}
        sentiment_datas.append(sentiment_data)

    return jsonify({"sentiments": sentiment_datas})


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
    end = target_date.replace(
        hour=23, minute=59, second=59, microsecond=999999).astimezone(pytz.UTC)

    return {'date_st': start, 'date_end': end}


def cal_datetime_kst(before_date, timezone='Asia/Seoul'):
    '''
    현재 일자에서 before_date 만큼 이전의 일자의 시작시간,끝시간 반환
    :param before_date: 이전일자
    :param timezone: 타임존
    :return: 해당일의 시작시간(date_st)과 끝 시간(date_end)
    :rtype: dict of datetime object
    :Example:
    2021-09-13 KST 에 get_date(1) 실행시,
    return은 {'date_st': datetype object 형태의 '2021-09-12 00:00:00+09:00'), 'date_end': datetype object 형태의 '2021-09-12 23:59:59.999999+90:00'}
    '''
    today = pytz.timezone(timezone).localize(datetime.now())
    target_date = today - timedelta(days=before_date)

    # 같은 일자 same date 의 00:00:00 로 변경
    start = target_date.replace(hour=0, minute=0, second=0,
                                microsecond=0)

    # 같은 일자 same date 의 23:59:59 로 변경
    end = target_date.replace(
        hour=23, minute=59, second=59, microsecond=999999)

    return {'date_st': start, 'date_end': end}


if __name__ == '__main__':
    app.run('0.0.0.0', port=5000, debug=True)
