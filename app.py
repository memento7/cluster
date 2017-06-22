#!/usr/local/bin/python3
from datetime import datetime, timedelta

import numpy as np
from KINCluster import KINCluster

from pipeline import PipelineServer
from connection import get_navernews, get_entities
from utility import get_similar, filter_quote, Logging, date_valid, start_cluster, close_cluster
import memento_settings as MS

#NEED_CONCAT = DATE_JUMP < DATE_RANGE
def process(entity: str,
            date_start: str,
            date_end: str,
            manage_id: str):
    Logging.register('kin', '<<KINCluster>>: {}')
    
    if date_valid(date_start) and date_valid(date_end):
        pass

    info_id = start_cluster(entity, date_start, date_end, manage_id)
    if not info_id:
        print ('already in process or done')
        return
    print (info_id,'started!')

    Logging.logf('kin', '{}_{}_{}'.format(entity, date_start, date_end))

    frame = get_navernews(entity, date_start, date_end)

    Logging.logf('kin', '{} has {} news'.format(entity, frame.shape[0]))

    if not frame.empty:
        simi_list = [" ".join([a, b, filter_quote(c), filter_quote(d)])
                    for a, b, c, d in zip(frame['title'],
                                          frame['content'],
                                          frame['title_quote'],
                                          frame['content_quote'])]
        frame.loc[:, 'similar'] = get_similar(simi_list, entity)

        rel_condition = (frame['similar'] > MS.MINIMUM_SIMILAR) & (frame['content'].str.contains(entity))
        rel_frame = frame.loc[rel_condition]
        rel_size, _ = rel_frame.shape

        Logging.logf('kin', '{} has related {} news'.format(entity, rel_size))

        if rel_size >= MS.MINIMUM_ITEMS:
            kin = KINCluster(PipelineServer(**{
                'entity': entity,
                'date_start': date_start,
                'date_end': date_end,
                'frame': rel_frame,
                'manage_id': manage_id,
            }), settings={
                'EPOCH': 256,
                'THRESHOLD': 1.10
            })
            kin.run()
            del kin

    close_cluster(info_id, date_start, date_end, manage_id)

if __name__ == '__main__':
    date_start = datetime(2000, 1, 1)
    date_end = datetime(2017, 5, 30)
    date_range = timedelta(days=30)
    date_jump = timedelta(days=15)
    keywords = []

    for keyword in keywords:
        for day in range(int((date_end - date_start) / date_jump)):
            date_s = date_start + date_jump * day
            date_e = date_s + date_range

            process(**{
                'keyword': keyword,
                'date_start': date_s.strftime("%Y.%m.%d"),
                'date_end': date_e.strftime("%Y.%m.%d"),
            })
