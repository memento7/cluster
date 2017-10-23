from itertools import chain
from datetime import datetime

from KINCluster import Pipeline
import numpy as np

from item import myItem as Item
import memento_settings as MS
from connection import put_cluster, put_bulk
from utility import get_similar, get_emotion, filter_quote, Logging

class PipelineServer(Pipeline):
    def __init__(self, entity, date_start, date_end, frame, manage_id):
        self.entity = entity
        self.date_start = date_start
        self.date_end = date_end
        self.frame = frame
        self.manage_id = manage_id
        self.clusters = []

    def __del__(self):
        print (len(self.clusters), 'inserted!')
        put_bulk([{
            '_index': 'memento',
            '_type': 'cluster',
            '_source': cluster
        } for cluster in self.clusters])

    def capture_item(self):
        items = []
        for _, row in self.frame.iterrows():
            items.append(Item(entities=" ".join(row.entities),
                              title=row.title,
                              content=row.content,
                              published_time=row.published_time,
                              reply_count=row.reply_count,
                              href_naver=row.href_naver,
                              cate=row.cate,
                              imgs=row.imgs,
                              comments=row.comments,
                              title_quote=row.title_quote,
                              content_quote=row.content_quote,
                              oid=row.oid,
                              aid=row.aid))
        return items

    def dress_item(self, ext):

        def get_memento_rate(items):
            return sum([item.reply_count + 1000 for item in items])
        
        def get_property(item):
            return " ".join([item.title, item.content, filter_quote(item.title_quote), filter_quote(item.content_quote)])

        def topic_rating(item, value):
            rating = value
            if self.entity in item.title:
                rating += 0.04
            for bound in [1000, 500, 250, 100, 50, 25]:
                if item.reply_count > bound:
                    rating += 0.001
                else:
                    break
            for bound in [1000, 2500, 5000, 10000]:
                if len(item.content) > bound:
                    rating -= 0.001
                else:
                    break
            return rating

        if len(ext.items) < MS.MINIMUM_CLUSTER: 
            return

        get_cc = lambda comments: map(lambda x: x['content'], comments)

        rate = get_memento_rate(ext.items)
        emot = get_emotion("\n".join(map(lambda x: " ".join(get_cc(x.comments)), ext.items)))
        
        simi = get_similar(list(map(get_property, ext.items)), self.entity)
        accuracy = np.std(simi) * 100

        comb = get_similar(list(map(get_property, ext.items)))
        simi -= np.abs(np.mean(comb) - comb)
        simi = [topic_rating(item, v) for v, item in zip(simi, ext.items)]

        topic = ext.items[np.argmax(simi)]

        self.clusters.append({
            'entity': self.entity,
            'date_start': datetime.strptime(self.date_start, '%Y.%m.%d'),
            'date_end': datetime.strptime(self.date_end, '%Y.%m.%d'),
            'manage_id': self.manage_id,
            'topic': topic.dump(),
            'accuracy': accuracy,
            'keywords': [{
                'keyword': keyword,
                'value': value,
            } for keyword, value in ext.keywords],
            'rate': rate,
            'items': ["{}_{}".format(item.oid, item.aid) for item in ext.items],
            'emot': [{
                'emotion': emo[0],
                'value': emo[1],
            } for emo in emot]
        })