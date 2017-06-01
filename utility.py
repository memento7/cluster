import logging

from collections import Counter
from itertools import chain

from imp import reload
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from konlpy.tag import Komoran

logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)
logging.getLogger("gensim").setLevel(logging.CRITICAL)
class Logging:
    reload(logging)
    __log = logging
    __log.basicConfig(format='%(levelname)s:%(message)s',
                      filename='cluster.log',
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

from connection import get_entities

ENTITIES = get_entities()
def get_similar(keyword, items):
    entity = " ".join([tag['tag'] for tag in ENTITIES[keyword]['tags'][:100]])
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix_train = tfidf_vectorizer.fit_transform([entity] + items)
    return cosine_similarity(tfidf_matrix_train[0:1], tfidf_matrix_train)[0][1:]

TAGGER = Komoran()
def get_emotion(text, entities, size=5):
    counter = Counter([(word, tag) for word, tag in TAGGER.pos(text)]).most_common()
    keywords = list(map(lambda x: (x[0][0], x[1]), filter(lambda x: x[0][1] == 'XR', counter)))
    tags = [map(lambda x: (x['tag'], x['value']), ENTITIES[entity]['tags']) for entity in entities]
    return list(filter(lambda x: x[0] not in chain(*tags), keywords))[:size]
