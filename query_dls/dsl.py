ES_DLS_01 = """
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
         "range" : {
            "log_time" : {
              "from" : "%sT00:00:00.000+09:00",
              "to" : "%sT23:59:59.999+09:00"
            }
          }
        },
        {
          "term": {
            "page_id": "/voice_text"
          }
        },
        {
          "terms": {
            "action_body.config": [
              "zapping",
              "vod-recom",
              "interest",
              "prepurchase",
              "dcmark",
              "noresult",
              "nolgs",
              "active"
            ]
          }
        },
        {
          "terms": {
            "action_id": [
              "page_show",
              "focus.voice_text.button",
              "click.voice_text.button"
            ]
          }
        }
      ]
    }
  },
  "aggs": {
    "config": {
      "terms": {
        "field": "action_body.config",
        "size": 1000
      },
      "aggs": {
        "show": {
          "terms": {
            "field": "action_id",
            "size": 1000
          },
          "aggs": {
            "stb": {
              "cardinality": {
                "field": "stb_id"
              }
            }
          }
        },
        "test": {
          "bucket_script": {
            "buckets_path": {
              "show_count": "show['page_show']>_count",
              "click_count": "show['click.voice_text.button']>_count",
              "stb_count": "show['page_show']>stb.value"
            },
            "script": "( params.click_count / params.show_count ) * 100"
          }
        }
      }
    }
  }
}
"""
ES_DLS_02 = """
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
         "range" : {
            "log_time" : {
              "from" : "%sT00:00:00.000+09:00",
              "to" : "%sT23:59:59.999+09:00"
            }
          }
        },
        {
          "term": {
            "page_id": "/voice_text"
          }
        },
        {
          "terms": {
            "action_body.config": [
              "zapping",
              "vod-recom",
              "interest",
              "prepurchase",
              "dcmark",
              "noresult",
              "nolgs",
              "active"
            ]
          }
        },
        {
          "terms": {
            "action_id": [
              "page_show",
              "focus.voice_text.button",
              "click.voice_text.button"
            ]
          }
        }
      ]
    }
  },
  "aggs": {
    "target": {
      "terms": {
        "field": "action_body.target",
        "size": 1000
      },
      "aggs": {
        "show": {
          "terms": {
            "field": "action_id",
            "size": 1000
          },
          "aggs": {
            "stb": {
              "cardinality": {
                "field": "stb_id"
              }
            }
          }
        },
        "test": {
          "bucket_script": {
            "buckets_path": {
              "show_count": "show['page_show']>_count",
              "click_count": "show['click.voice_text.button']>_count",
              "stb_count": "show['page_show']>stb.value"
            },
            "script": "( params.click_count / params.show_count ) * 100"
          }
        }
      }
    }
  }
}
"""