from elasticsearch import Elasticsearch
import datetime
import pymysql
import pandas as pd
import sys

config = {
    "SCHDB_IP": "121.125.71.189", "SCHDB_PORT": 3306, "SCHDB_USER": "nudge_admin", "SCHDB_PASSWD": "admin!#189", "SCHDB_DB": "nudge_admin"
}

header = ["일자",
          "넛지종류",
          "노출문구",
          "노출위치",
          "넛지노출수",
          "넛지포커스수",
          "넛지 클릭수",
          "노출 대비 포커스",
          "포커스 대비 클릭",
          "클릭전환율",
          "노출 STB수",
          "포커스 STB수",
          "클릭 STB수",
          "노출 대비 포커스 STB",
          "포커스 대비 클릭 STB",
          "이용율"]
dataField = ["search_dt",
             "config",
             "man_text",
             "exposure_name",
             "show_count",
             "focus_count",
             "click_count",
             "show_focus",
             "focus_click",
             "show_click_rate",
             "show_stb_count",
             "focus_stb_count",
             "click_stb_count",
             "show_stb_focus",
             "focus_stb_click",
             "show_click_stb_rate"]


def makeParam(target_day):
    param = {"query": "", "time_zone": "Asia/Seoul", "fetch_size": 5000}
    param["query"] = f"""
    SELECT
      CONCAT(
            CONCAT(
               CONCAT(SUBSTRING(DATE_PART('year',format_dt)::string, 3,4) ,'.')
               , CASE
                  WHEN DATE_PART('month',format_dt) < 10 THEN CONCAT('0',DATE_PART('month',format_dt)::string)
                     ELSE DATE_PART('month',format_dt)::string
                   END
             ),
             CONCAT(
                    '.', CASE
                        WHEN DATE_PART('day',format_dt) < 10 THEN CONCAT('0',DATE_PART('day',format_dt)::string)
                        ELSE DATE_PART('day',format_dt)::string
                    END
                )
         ) search_dt
      ,man_text
      ,config
      ,target
         ,IFNULL( cast(   click_count       as integer )   ,0)   click_count
         , IFNULL( cast(   click_stb_count    as integer )   ,0)   click_stb_count
         , IFNULL( cast(   focus_count       as integer )   ,0)   focus_count
         , IFNULL( cast(   focus_stb_count    as integer )   ,0)   focus_stb_count
         , IFNULL( cast(   show_count       as integer )   ,0)   show_count
         , IFNULL( cast(   show_stb_count    as integer )    ,0)   show_stb_count
         , CASE
             WHEN IFNULL( cast(  show_count       as float )      ,0)  = 0 then 0
             ELSE
               ROUND(
                  IFNULL( cast(  click_count        as float )      ,0) /
                  IFNULL( cast(  show_count       as float )      ,0) * 100
                 , 2
               )
            END show_click_rate
          , CASE
             WHEN IFNULL( cast(  show_stb_count       as float )      ,0)  = 0 then 0
             ELSE
               ROUND(
                  IFNULL( cast(  click_stb_count        as float )      ,0) /
                  IFNULL( cast(  show_stb_count       as float )      ,0) * 100
                 , 2
               )
            END show_click_stb_rate
      FROM (
         SELECT
            DATE_TRUNC('day', log_time) format_dt
             ,action_body.voice_name
             man_text
            ,action_body.config config
            ,action_body.target1 target
              ,count(
                distinct (
                  case
                    when action_id  = 'page_show'  then stb_id
                    else null
                  end
                )
              )  show_stb_count
              , count(
                   distinct (
                     case
                    when action_id  = 'focus.voice_text.button'  then stb_id
                    else null
                  end
                )
              )  focus_stb_count
              , count(
                   distinct (
                     case
                    when action_id  = 'click.voice_text.button'  then stb_id
                    else null
                  end
                )
              )  click_stb_count
                 , sum(
                   case
                  when action_id  = 'page_show'  then 1
                  else 0
                end
              )  show_count
              ,SUM(
                case
                  when action_id  = 'focus.voice_text.button'  then 1
                  else 0
                end
              )  focus_count
              ,SUM(
                case
                  when action_id  = 'click.voice_text.button'  then 1
                  else 0
                end
              )  click_count
          FROM "index-nudge-result-union"
          WHERE
             log_time between '{target_day}T00:00:00.000+09:00' AND '{target_day}T23:59:59.999+09:00'
             and action_body.config in (
                 'break_time'
                ,'assign_contents'
                ,'text'
                ,'search_keyword'
                ,'view_text'
             )
          group by format_dt , man_text ,action_body.target1 ,action_body.config
      )
      order by format_dt , man_text, target
    """
    return param


def searchEsSql(param):
    cols = None
    isFirst = True
    colList = []
    dataList = []
    es = Elasticsearch(["121.125.71.147",
                        "121.125.71.148",
                        "121.125.71.149"],
                       port=9200,
                       http_auth=("elastic", "wtlcnNyrDPVko01lZfIl"),
                       timeout=200)

    while (isFirst or param["cursor"] != None):
        # param["cursor"] = cursor
        res = es.sql.query(body=param)
        if res != None:
            if "cursor" in res:
                param["cursor"] = res["cursor"]
                del param["query"]
            else:
                param["cursor"] = None

            if (cols == None):
                cols = res["columns"]
                for col in cols:
                    colList.append(col["name"])

            if (res["rows"] != None):
                for row in res["rows"]:
                    # print(row)
                    dataList.append(dict(zip(colList, row)))
        else:
            param["cursor"] = None

        isFirst = False

    return dataList


def searchDBData():
    con_pymysql = pymysql.connect(host=config['SCHDB_IP'], port=config['SCHDB_PORT'], user=config['SCHDB_USER'],
                                  password=config['SCHDB_PASSWD'],
                                  db=config['SCHDB_DB'], charset='utf8', read_timeout=20)

    cur_pymysql = con_pymysql.cursor(pymysql.cursors.DictCursor)

    cur_pymysql.execute("select nudge_type, nudge_name from nudge n  ")
    nudge_rows = cur_pymysql.fetchall()
    nudge_map = {}
    for nudge in nudge_rows:
        nudge_map[nudge["nudge_type"]] = nudge["nudge_name"]

    # cur_pymysql.execute("select `type`, suggest_name from suggest ")
    # suggest_rows = cur_pymysql.fetchall()
    # suggest_map = {}
    # for suggest in suggest_rows:
    #     suggest_map[suggest["type"]] = suggest["suggest_name"]

    cur_pymysql.execute("""
      select 
        menu_id,
        case 
          when menu_id = 'menu003' or menu_id = 'menu004'  then '시놉시스'
          else  exposure_name 
        end
        exposure_name 
      from exposure
    """)
    expo_rows = cur_pymysql.fetchall()
    expo_map = {}
    for expo in expo_rows:
        expo_map[expo["menu_id"]] = expo["exposure_name"]

    cur_pymysql.close()
    con_pymysql.close()

    return {"EXPO": expo_map, "NUDGE": nudge_map}


if __name__ == '__main__':
    # sys.argv.append('2021-04-30')
    # sys.argv.append('25')
    print('start ===')
    startDay = datetime.datetime.now()
    loop = 1

    if len(sys.argv) > 1:
        startDay = datetime.date.fromisoformat(sys.argv[1])

    if len(sys.argv) > 2:
        loop = int(sys.argv[2])

    writer = pd.ExcelWriter('./src/manual_nudge_data.xlsx')

    dbData = searchDBData()
    expo_map = dbData["EXPO"]
    nudge_map = dbData['NUDGE']

    execl_rows = []
    for i in range(loop):
        target_day_str = f"{startDay.strftime('%Y-%m-%d')}"
        print(target_day_str)
        param = makeParam(target_day_str)
        dataList = searchEsSql(param)

        for data in dataList:
            data['config'] = nudge_map[data['config']]
            if data["target"] in expo_map:
                data["exposure_name"] = expo_map[data["target"]]
            else:
                data["exposure_name"] = data["target"]

            execl_row = []
            for dfi in dataField:
                # 노출 대비 포커스
                try:
                    show_focus = f'{((data["focus_count"] / data["show_count"]) * 100)}'
                except ZeroDivisionError:
                    show_focus = f'{0}'
                # 포커스 대비 클릭
                try:
                    focus_click = f'{((data["click_count"] / data["focus_count"]) * 100)}'
                except ZeroDivisionError:
                    focus_click = f'{0}'
                # 클릭 전환율
                try:
                    show_click_rate = f'{(data["click_count"] / data["show_count"]) * 100}'
                except ZeroDivisionError:
                    show_click_rate = f'{0}'

                # 노출 대비 포커스
                try:
                    show_focus_stb = f'{((int(data["focus_stb_count"]) / data["show_stb_count"]) * 100)}'
                except ZeroDivisionError:
                    show_focus_stb = f'{0}'
                # 포커스 대비 클릭
                try:
                    focus_click_stb = f'{((data["click_stb_count"] / int(data["focus_stb_count"])) * 100)}'
                except ZeroDivisionError:
                    focus_click_stb = f'{0}'
                # 이용율
                try:
                    show_click_stb_rate = f'{(data["click_stb_count"] / data["show_stb_count"]) * 100}'
                except ZeroDivisionError:
                    show_click_stb_rate = f'{0}'

                data["show_focus"] = '{:.1f}%'.format(float(show_focus))
                data["focus_click"] = '{:.0f}%'.format(float(focus_click))
                data["show_click_rate"] = '{:.2f}%'.format(float(show_click_rate))
                data["show_stb_focus"] = '{:.1f}%'.format(float(show_focus_stb))
                data["focus_stb_click"] = '{:.0f}%'.format(float(focus_click_stb))
                data["show_click_stb_rate"] = '{:.2f}%'.format(float(show_click_stb_rate))

                execl_row.append(data[dfi])

            execl_rows.append(execl_row)

        startDay = startDay + datetime.timedelta(days=1)

    sheetName = "Sheet1"
    try:
        df = pd.DataFrame(list(execl_rows), columns=header)
        df.to_excel(writer, sheetName, index=False)
    except ValueError as err:
        print(execl_rows)
        print(header)
        print(err)

    writer.save()
    writer.close()
    print('end ===')
