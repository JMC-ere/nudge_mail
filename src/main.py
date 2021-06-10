import json
from db_connect_data import db_connect, type_db, location_db
from es_connect_data import es_connect, type_es, location_es
from make_xl import insert_data


if __name__ == '__main__':
    with open('../config/info.json', 'r') as connect_info:
        info = json.load(connect_info)
    info = info['STG']

    # start_days = ['2021-04-30', '2021-05-01', '2021-05-02', '2021-05-03', '2021-05-04', '2021-05-05', '2021-05-06',
    #               '2021-05-07', '2021-05-08', '2021-05-09', '2021-05-10', '2021-05-11', '2021-05-12', '2021-05-13',
    #               '2021-05-14', '2021-05-15', '2021-05-16', '2021-05-17', '2021-05-18', '2021-05-19', '2021-05-20',
    #               '2021-05-21', '2021-05-22', '2021-05-23', '2021-05-24', '2021-05-25']

    start_days = ['2021-06-09']

    result_type_data = []
    result_location_data = []

    # DB Connection
    db_info = db_connect(info)
    # 종류별 DB 데이터
    type_data = type_db(db_info)
    # 위치별 DB 데이터
    location_data = location_db(db_info)

    # ES Connection
    es_client = es_connect(info)
    for start_day in start_days:
        # 종류별 ES 데이터
        type_es_data = type_es(es_client, info, start_day)
        # 위치별 ES 데이터
        location_es_data = location_es(es_client, info, start_day)

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
            print(location_e)
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

