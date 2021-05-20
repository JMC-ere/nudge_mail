from elasticsearch import Elasticsearch
import datetime
import pymysql
import pandas as pd
import sys

config = {
    "SCHDB_IP": "1.255.144.64", "SCHDB_PORT": 3306, "SCHDB_USER": "nudge_1", "SCHDB_PASSWD": "0000", "SCHDB_DB": "nudge"
}

header = ["일자", "노출문구", "노출위치",
          "넛지노출수", "넛지포커스수", "넛지 클릭수", "클릭전환율", "노출 STB수", "포커스 STB수", "클릭 STB수", "이용율"]
dataField = ["search_dt", "man_text", "exposure_name", "show_count", "focus_count",
             "click_count", "show_click_rate", "show_stb_count", "focus_stb_count", "click_stb_count",
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
             , case
              when action_body.menu_name  is null then action_body.voice.keyword
              else action_body.menu_name
            end man_text
            ,action_body.target target
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
          FROM "index-nudge-result-analysis"
          WHERE
             log_time between '{target_day}T00:00:00.000+09:00' AND '{target_day}T23:59:59.999+09:00'
             and action_body.config in (
                  'break_time'
            ,'assign_contents'
            ,'text'
            ,'search_keyword'
            ,'view_text'
             )
            and stb_id not in ('A8385AE5-57A4-11EA-B1D9-BF69B691B61A', 'BB56F5DA-57A5-11EA-B1D9-BF69B691B61A')
          group by format_dt , man_text ,action_body.target
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
    # cur_pymysql.execute("select nudge_type, nudge_name from nudge n  ")
    # nudge_rows = cur_pymysql.fetchall()
    # nudge_map = {}
    # for nudge in nudge_rows:
    #     nudge_map[nudge["nudge_type"]] = nudge["nudge_name"]

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

    return {"EXPO": expo_map}


if __name__ == '__main__':
    print('start ===')
    startDay = datetime.datetime.now()
    loop = 1

    if len(sys.argv) > 1:
        startDay = datetime.date.fromisoformat(sys.argv[1])

    if len(sys.argv) > 2:
        loop = int(sys.argv[2])

    writer = pd.ExcelWriter('수동넛지_통계데이터.xlsx')

    dbData = searchDBData()
    expo_map = dbData["EXPO"]

    for i in range(loop):
        target_day_str = f"{startDay.strftime('%Y-%m-%d')}"
        print(target_day_str)
        param = makeParam(target_day_str)
        dataList = searchEsSql(param)

        execl_rows = []
        for data in dataList:
            if data["target"] in expo_map:
                data["exposure_name"] = expo_map[data["target"]]
            else:
                data["exposure_name"] = data["target"]

            execl_row = []
            for dfi in dataField:
                execl_row.append(data[dfi])

            execl_rows.append(execl_row)

        sheetName = f"{startDay.strftime('%m월%d일')}"
        df = pd.DataFrame(execl_rows, columns=header)
        df.to_excel(writer, sheetName, index=False)

        startDay = startDay + datetime.timedelta(days=1)

    writer.save()
    print('end ===')
