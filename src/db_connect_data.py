import pymysql
from query_dls import qry


def db_connect(info):
    try:
        conn = pymysql.connect(host=info['DB_HOST'],
                               user=info['DB_USER'],
                               password=info['DB_PWD'],
                               db=info['DB_NAME'],
                               charset='utf8', cursorclass=pymysql.cursors.DictCursor)
        return conn
    except Exception as err:
        print(err)
        quit()


# 종류 별
def type_db(conn):
    try:
        type_curs = conn.cursor()

        type_sql = qry.NUDGE_TYPE
        type_curs.execute(type_sql)

        rows = type_curs.fetchall()

        type_list_data = []

        for row in rows:
            dict_data = {row['nudge_type']: row['nudge_name']}
            type_list_data.append(dict_data)

        return type_list_data

    except Exception as err:
        print(err)
        quit()


# 위치 별
def location_db(conn):
    try:
        location_curs = conn.cursor()

        location_sql = qry.MENU_ID
        location_curs.execute(location_sql)

        rows = location_curs.fetchall()

        type_list_data = []

        for row in rows:
            dict_data = {row['menu_id']: row['exposure_name']}
            type_list_data.append(dict_data)

        return type_list_data

    except Exception as err:
        print(err)
        quit()