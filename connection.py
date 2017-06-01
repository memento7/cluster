from os import environ
from time import sleep
from typing import Union
import json

import memento_settings as MS

from elasticsearch import Elasticsearch

import requests
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

ES = Elasticsearch(**MS.SERVER_ES_INFO)

def make_clear(results: list,
               key_lambda = lambda x: x['_id'],
               value_lambda = lambda x: x,
               filter_lambda = lambda x: x) -> dict:
    def clear(result) -> dict:
        result['_source']['_id'] = result['_id']
        return result['_source']
    iterable = filter(filter_lambda, map(clear, results))
    return {key_lambda(source): value_lambda(source) for source in iterable}

def get_scroll(query={}, doc_type='', index='information'):
    array = []
    def _get_scroll(scroll) -> Union[int, list]:
        doc = scroll['hits']['hits']
        array.extend(doc)
        return doc and True or False
    scroll = ES.search(index=index, doc_type=doc_type, body=query, scroll='1m', size=1000)
    scroll_id = scroll['_scroll_id']
    while _get_scroll(scroll):
        scroll = ES.scroll(scroll_id=scroll_id, scroll='1m')
    return make_clear(array)

def get_entities() -> dict:
    return get_scroll({}, 'namugrim')

def get_navernews(keyword, date_start, date_end):
    news = get_scroll({
                'query': {
                    'bool': {
                        'must': [{
                            'range': {
                                'published_time': {
                                    "gte" : date_start,
                                    "lte" : date_end,
                                    "format": "yyyy.MM.dd",
                                }
                            }
                        }, {
                            'term': {
                                'information.keyword': keyword,
                            }
                        }]
                    }
                }
            }, index='news_naver')

    def info_chain(x):
        x['keyword'] = x['information']['keyword']
        x['subkey'] = x['information']['subkey']
        del x['information']
        return x

    return pd.DataFrame(list(map(info_chain, news.values())))

def get_type_id(query: list, type: str) -> str:
    wrapper = lambda x: {'match': {x[0]: x[1]}}
    result = ES.search(index='information', doc_type=type, body={
        'query': {
            'bool': {
                'must': list(map(wrapper, query.items()))
            }
        }
    })
    if result['hits']['total']:
        return result['hits']['hits'][0]['_id']
    result = ES.index(
        index='information',
        doc_type=type,
        body=query
    )
    return result['_id']

def put_item(item: dict, doc_type:str, index: str):
    while True:
        try:
            result = ES.index(
                index=index,
                doc_type=doc_type,
                body=item
            )
            break
        except: 
            print ('Connection Error wait for 2s')
            sleep(2)
            continue
    print (index, doc_type, result['_id'])
    return result['_id']

# Default Connection Function

def put_cluster(cluster, keyword, date_start, date_end):
    cluster_id = put_item({
        'keyword': keyword,
        'date_start': date_start,
        'date_end': date_end,
    }, doc_type='cluster', index='information')
    print (cluster_id)
    put_item(cluster, doc_type=cluster_id, index='cluster')
