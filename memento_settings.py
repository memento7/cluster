# Custom Settings
from os import environ

SERVER_ES = 'server2.memento.live'
SERVER_ES_INFO = {
    'host': SERVER_ES,
    'port': 9200,
    'http_auth': (environ['MEMENTO_ELASTIC'], environ['MEMENTO_ELASTIC_PASS'])
}

MINIMUM_ITEMS = 6
MINIMUM_CLUSTER = 3

MINIMUM_SIMILAR = 0.0225
