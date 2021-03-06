import logging
from collections import Counter
from socket import gethostbyname, gethostname
from itertools import chain
from datetime import datetime
from time import sleep

from imp import reload
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from konlpy.tag import Komoran

from connection import get_exist, get_item, put_item, update_item

class Logging:
    reload(logging)
    __log = logging
    __log.basicConfig(format='%(levelname)s:%(message)s',
                      filename='cluster.log',
                      filemode='a',
                      level=logging.INFO)
    __logger = {
        'INFO': __log.info,
        'DEBUG': __log.debug,
        'warning': __log.warning
    }
    __logger_format = {
    }

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        Logging.__log.info('Logging: {} with args: {}, kwargs: {}'.format(
            self.func.__name__, args, kwargs,
        ))
        ret = self.func(*args, **kwargs)
        Logging.__log.info('Logging: {} returns {}'.format(
            self.func.__name__, type(ret),
        ))
        return ret

    def __get__(self, label):

        pass

    @staticmethod
    def register(label, format):
        Logging.__logger_format[label] = format

    @staticmethod
    def logf(label, msg, debug=True):
        Logging.log(Logging.__logger_format[label].format(msg))
        if debug:
            print (Logging.__logger_format[label].format(msg))
            
    @staticmethod
    def log(msg, level='INFO'):
        Logging.__logger[level](msg)

def filter_quote(quotes):
    return " ".join(["".join(quote) for quote in quotes])

def date_valid(date_text):
    try:
        datetime.strptime(date_text, '%Y.%m.%d')
        return True
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY.MM.DD")

def now():
    return str(datetime.now())[:19]

def get_tag_info(entity):
    entity_info = get_item(index='memento',doc_type='entities',idx=entity)
    if 'flag' not in entity_info:
        return []
    return get_item(index='memento',doc_type='namugrim',idx=entity_info['flag'])['tags']

def get_similar(items, keyword=None):
    if keyword:
        items = [" ".join([tag['tag'] for tag in get_tag_info(keyword)[:100]])] + items

    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix_train = tfidf_vectorizer.fit_transform(items)
    result = cosine_similarity(tfidf_matrix_train[0:1], tfidf_matrix_train)[0]
    return result[1:] if keyword else result

TAGGER = Komoran()
def get_emotion(text, size=10):
    counter = Counter([(word, tag) for word, tag in TAGGER.pos(text)]).most_common()
    keywords = list(map(lambda x: (x[0][0], x[1]), filter(lambda x: x[0][1] == 'XR', counter)))
    return keywords[:size] if keywords else [] 

def start_cluster(entity, date_start, date_end, manage_id):
    idx = put_item({
        'client': gethostname(),
        'start_time': now(),
        'update_time': now(),
        'date_start': date_start,
        'date_end': date_end,
        'manage_id': manage_id,
        'finish': 'false',
    }, doc_type='cluster', index='memento_info')
    return idx

def close_cluster(info_id, date_start, date_end, manage_id):
    result = get_item(info_id, doc_type='cluster', index='memento_info')
    if not result:
        print('cant find start info')
        return

    if result['date_start'] != date_start or result['date_end'] != date_end or result['manage_id'] != manage_id:
        print ('error, start info do not match end info')
        return

    update_item({
        'doc': {
            'update_time': now(),
            'finish': 'true',
        }
    }, info_id, doc_type='cluster', index='memento_info')
