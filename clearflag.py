import pickle

from elasticsearch import Elasticsearch
es = Elasticsearch(host='server2.memento.live', http_auth=('elastic', 'elastic@memento#'))

clusters = pickle.load(open('clusters.p','rb'))
for cluster in clusters:
    es.update(index='memento',doc_type='cluster',id=cluster['_id'],body={
        'doc': {
            'task': 'done'
        }
    })