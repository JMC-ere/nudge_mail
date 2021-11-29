import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import json
from db_connect_data import db_connect, type_db, location_db
from es_connect_data import es_connect, type_es, location_es
from make_xl import insert_data
from datetime import datetime, timedelta
from query_dsl import qry, dsl

if __name__ == '__main__':

    print(sys.path)

    start_days = []
    start = sys.argv[1]
    loop = int(sys.argv[2])

    for i in range(0, loop):
        day = datetime.strptime(start, '%Y-%m-%d') + timedelta(days=i)
        day = day.strftime('%Y-%m-%d')
        start_days.append(day)

    print(start_days)

    with open('config/info.json', 'r') as connect_info:
        info = json.load(connect_info)
    info = info['PRD']

    # start_days = ['2021-06-01', '2021-06-02', '2021-06-03', '2021-06-04', '2021-06-05', '2021-06-06',
    #               '2021-06-07', '2021-06-08', '2021-06-09', '2021-06-10',
    #               '2021-06-11', '2021-06-12', '2021-06-13', '2021-06-14', '2021-06-15', '2021-06-16', '2021-06-17',
    #               '2021-06-18', '2021-06-19', '2021-06-20',
    #               '2021-06-21', '2021-06-22', '2021-06-23', '2021-06-24', '2021-06-25', '2021-06-26', '2021-06-27',
    #               '2021-06-28', '2021-06-29', '2021-06-30']

    result_type_data = []
    result_location_data = []

    # DB Connection
    db_info = db_connect(info)
    # 종류별 DB 데이터
    type_data = type_db(db_info, qry)
    # 위치별 DB 데이터
    location_data = location_db(db_info, qry)

    # ES Connection
    es_client = es_connect(info)
    for start_day in start_days:
        # 종류별 ES 데이터
        type_es_data = type_es(es_client, info, start_day, dsl=dsl)
        # 위치별 ES 데이터
        location_es_data = location_es(es_client, info, start_day, dsl=dsl)

        # DB 데이터를 ES 데이터와 비교하여 일치할경우 데이터 추가
        for type_e in type_es_data:
            for type_d in type_data:
                db_type_name = list(dict(type_d).keys())[0]
                db_type_value = list(dict(type_d).values())[0]
                if db_type_name in type_e['type_name']:
                    type_e['type_value'] = db_type_value

            result_type_data.append(type_e)
        # DB 데이터를 ES 데이터와 비교하여 일치할경우 데이터 추가
        for location_e in location_es_data:
            for location_d in location_data:
                db_location_name = list(dict(location_d).keys())[0]
                db_location_value = list(dict(location_d).values())[0]
                if db_location_name in location_e['menu_name']:
                    location_e['location_value'] = db_location_value
            result_location_data.append(location_e)

    insert_data(location_data=result_location_data,
                type_data=result_type_data)

    db_info.close()
    es_client.close()

