#!/usr/bin/env python
"""
title:           A CLI tool for exporting data from Elasticsearch into a CSV file.
description:     Command line utility, written in Python, for querying Elasticsearch in Lucene query syntax or Query DSL syntax and exporting result as documents into a CSV file.
usage:           es2csv -q '*' -i _all -e -o ~/file.csv -k -m 100
                 es2csv -q '{"query": {"match_all": {}}}' -r -i _all -o ~/file.csv
                 es2csv -q @'~/long_query_file.json' -r -i _all -o ~/file.csv
                 es2csv -q '*' -i logstash-2015-01-* -f host status message -o ~/file.csv
                 es2csv -q 'host: localhost' -i logstash-2015-01-01 logstash-2015-01-02 -f host status message -o ~/file.csv
                 es2csv -q 'host: localhost AND status: GET' -u http://kibana.com:80/es/ -o ~/file.csv
                 es2csv -q '*' -t dev prod -u http://login:password@kibana.com:6666/es/ -o ~/file.csv
                 es2csv -q '{"query": {"match_all": {}}, "filter":{"term": {"tags": "dev"}}}' -r -u http://login:password@kibana.com:6666/es/ -o ~/file.csv
"""
import os
import sys
import time
from operator import itemgetter
import argparse
import json
import csv
import elasticsearch
import progressbar
from functools import wraps
from datetime import datetime

FLUSH_BUFFER = 1000  # Chunk of docs to flush in temp file
CONNECTION_TIMEOUT = 120
TIMES_TO_TRY = 3
RETRY_DELAY = 60
META_FIELDS = ['_id', '_index', '_score', '_type']
__version__ = '5.2.1'


# Retry decorator for functions with exceptions
def retry(ExceptionToCheck, tries=TIMES_TO_TRY, delay=RETRY_DELAY):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries = tries
            while mtries > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    print(e)
                    print('Retrying in %d seconds ...' % delay)
                    time.sleep(delay)
                    mtries -= 1
                else:
                    print('Done.')
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck as e:
                print('Fatal Error: %s' % e)
                exit(1)

        return f_retry

    return deco_retry


class Es2csv:

    def __init__(self, index='*', host='91.235.136.166', port=9200, ):
        self.host = host
        self.port = port
        self.index = index
        self.num_results = 0
        self.scroll_ids = []
        self.scroll_time = '30m'

        # self.csv_headers = list(META_FIELDS) if self.opts.meta_fields else []
        # self.tmp_file = '%s.tmp' % opts.output_file

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def create_connection(self):
        es = elasticsearch.Elasticsearch(self.host, timeout=CONNECTION_TIMEOUT)
        es.cluster.health()
        self.es = es

    @retry(elasticsearch.exceptions.ConnectionError, tries=TIMES_TO_TRY)
    def check_indexes(self):
        indexes = self.es.indices.get_alias(self.index)

        for index in indexes:
            self.dump_index(index)

    def dump_index(self, index):
        page = self.es.search(
            index=index,
            scroll=self.scroll_time,
            size=1000,
            body={
                "query": {
                    "match_all": {}
                }
            },
            sort='_id'
        )
        sid = page['_scroll_id']
        scrolled = len(page['hits']['hits'])
        scroll_size = page['hits']['total']

        widgets = ['Exporting {index}'.format(index=str(index)),
                   progressbar.Bar(left='[', marker='#', right=']'),
                   progressbar.FormatLabel(' [%(value)i/%(max)i] ['),
                   progressbar.Percentage(),
                   progressbar.FormatLabel('] [%(elapsed)s] ['),
                   progressbar.ETA(), '] [',
                   progressbar.FileTransferSpeed(unit='docs'), ']'
                   ]
        bar = progressbar.ProgressBar(widgets=widgets, maxval=scroll_size).start()
        bar.update(scrolled)

        if self.prepare_file(index, page['hits']['hits']) is None:
            return

        self.write_to_file(page['hits']['hits'])

        while (scrolled < scroll_size):
            # print ("Scrolling...")
            page = self.es.scroll(scroll_id=sid, scroll=self.scroll_time)
            # Update the scroll ID
            sid = page['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scrolled += len(page['hits']['hits'])
            bar.update(scrolled)

            self.write_to_file(page['hits']['hits'])

    def prepare_file(self, index, data, type='json'):

        if len(data) == 0:
            return None

        self.filename = '{index}_{time}.{type}'.format(index=index.replace('.', '_'),
                                                       time=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                                                       type=type)
        file = open(self.filename, 'w')

        if type == 'json':
            pass

        if type == 'csv':
            self.csv_headers = get_headers(data[0]['_source'])
            file.write(','.join(self.csv_headers))

        self.file_type = type

        file.close()
        return 1

    def write_to_file(self, data):

        if self.file_type == 'json':
            with open(self.filename, 'a') as file:
                for hit in data:
                    file.write(json.dumps(hit['_source'])+'\n')
                file.close()

        if self.file_type == 'csv':
            self.csv_headers = []

            def to_keyvalue_pairs(source, ancestors=[], header_delimeter=','):
                def is_list(arg):
                    return type(arg) is list

                def is_dict(arg):
                    return type(arg) is dict

                if is_dict(source):
                    for key in source.keys():
                        to_keyvalue_pairs(source[key], ancestors + [key])

                elif is_list(source):
                    # if self.opts.kibana_nested:
                    [to_keyvalue_pairs(item, ancestors) for item in source]
                    # else:
                    #     [to_keyvalue_pairs(item, ancestors + [str(index)]) for index, item in enumerate(source)]
                else:
                    header = header_delimeter.join(ancestors)
                    if header not in self.csv_headers:
                        self.csv_headers.append(header)
                    try:
                        out[header] = '%s%s%s' % (out[header], ',', source)
                    except:
                        out[header] = source

            with open(self.filename, 'a') as tmp_file:
                for hit in data:
                    out = {}
                    if '_source' in hit and len(hit['_source']) > 0:
                        to_keyvalue_pairs(hit['_source'])
                        tmp_file.write('%s\n' % json.dumps(out))
                tmp_file.close()


def main():

    es = Es2csv(index='usdbch.kraken.*')
    es.create_connection()
    es.check_indexes()

if __name__ == '__main__':
    main()
