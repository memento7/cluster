from itertools import chain
import json

from KINCluster import Pipeline
import numpy as np
import pickle

from utility import get_similar, get_emotion, filter_quote, Logging
from item import myItem as Item
from connection import put_cluster
import memento_settings as MS

class PipelineServer(Pipeline):
    def __init__(self, keyword, date_start, date_end, frame):
        self.keyword = keyword
        self.date_start = date_start
        self.date_end = date_end
        self.frame = frame

    def capture_item(self):
        items = []
        for _, row in self.frame.iterrows():
            items.append(Item(keyword=row.keyword,
                              title=row.title,
                              content=row.content,
                              published_time=row.published_time,
                              reply_count=row.reply_count,
                              href_naver=row.href_naver,
                              cate=row.cate,
                              imgs=row.imgs,
                              comments=row.comments,
                              title_quote=row.title_quote,
                              content_quote=row.content_quote))
        return items

    def dress_item(self, ext):

        def get_memento_rate(items):
            return sum([item.reply_count + 1000 for item in items])
        
        def get_property(item):
            return " ".join([item.title, item.content, filter_quote(item.title_quote), filter_quote(item.content_quote)])

        if len(ext.items) < MS.MINIMUM_CLUSTER: 
            return

        get_cc = lambda comments: map(lambda x: x['content'], comments)

        rate = get_memento_rate(ext.items)
        emot = get_emotion("\n".join(map(lambda x: " ".join(get_cc(x.comments)), ext.items)), [self.keyword])
        simi = get_similar(self.keyword, list(map(get_property, ext.items)))
        simi = [(self.keyword in item.title and v + 0.03 or v) for v, item in zip(simi, ext.items)]
        topic = ext.items[np.argmax(simi)]

        put_cluster({
            'keyword': self.keyword,
            'date_start': self.date_start,
            'date_end': self.date_end,
            'topic': topic.dump(),
            'keywords': [{
                'keyword': keyword,
                'value': value,
            } for keyword, value in ext.keywords],
            'rate': rate,
            'items': [item.dump() for item in ext.items],
            'emot': [{
                'emotion': emo[0],
                'value': emo[1],
            } for emo in emot]
        }, **{
            'keyword': self.keyword,
            'date_start': self.date_start,
            'date_end': self.date_end,
        })
