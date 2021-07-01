from elasticsearch import Elasticsearch
from query_dls import dsl
import traceback


def es_connect(info):
    try:

        es_client = Elasticsearch([info['ANALYSIS_1'],
                                   info['ANALYSIS_2'],
                                   info['ANALYSIS_3']],
                                  timeout=200,
                                  port=info['PORT'],
                                  http_auth=(info['ID'], info['PWD']))
        return es_client

    except Exception as err:
        print(err)
        print(traceback.format_exc())
        quit()


# 종류 별
def type_es(es_client, info, start_day):
    try:
        type_es_data = es_client.search(index=info['INDEX_NAME'], body=dsl.ES_DLS_01 % (start_day, start_day))
        type_data = type_es_data['aggregations']['config']
        type_list_buckets = []
        list_type_data = []

        if "buckets" in type_data:
            for buckets in type_data['buckets']:
                type_list_buckets.append(buckets)

        for buckets in type_list_buckets:

            dict_type = {'type_name': buckets['key'], 'day': str(start_day)}

            for in_buckets in buckets['show']['buckets']:
                dict_in_buckets = dict(in_buckets)

                dict_type[dict_in_buckets['key']] = dict_in_buckets['doc_count']
                dict_type[dict_in_buckets['key'] + "_stb"] = dict_in_buckets['stb']['value']

            list_type_data.append(dict_type)

        return list_type_data

    except Exception as err:
        print(err)
        print(traceback.format_exc())
        quit()


# 위치 별
def location_es(es_client, info, start_day):
    try:
        location_es_data = es_client.search(index=info['INDEX_NAME'], body=dsl.ES_DLS_02 % (start_day, start_day))

        location_data = location_es_data['aggregations']['target']
        location_list_buckets = []
        list_location_data = []

        if "buckets" in location_data:
            for buckets in location_data['buckets']:
                location_list_buckets.append(buckets)

        for buckets in location_list_buckets:

            dict_location = {'menu_name': buckets['key'], 'day': str(start_day)}

            for in_buckets in buckets['show']['buckets']:
                dict_in_buckets = dict(in_buckets)

                dict_location[dict_in_buckets['key']] = dict_in_buckets['doc_count']
                dict_location[dict_in_buckets['key'] + "_stb"] = dict_in_buckets['stb']['value']

            list_location_data.append(dict_location)

        return list_location_data

    except Exception as err:
        print(err)
        print(traceback.format_exc())
        quit()
