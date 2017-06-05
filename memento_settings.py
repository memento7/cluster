# Custom Settings
from os import environ

SERVER_ES = 'server2.memento.live'
SERVER_ES_INFO = {
    'host': SERVER_ES,
    'port': 9200,
}
SERVER_API = 'https://api.memento.live/persist/'
SERVER_API_HEADER = { 
    "Content-Type" : "application/json",
    "charset": "utf-8",
    "Authorization": environ['MEMENTO_BASIC']
}

MINIMUM_ITEMS = 8
MINIMUM_CLUSTER = 4

MINIMUM_SIMILAR = 0.03
