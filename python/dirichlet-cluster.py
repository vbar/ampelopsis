#!/usr/bin/python3

import csv
import json
import numpy as np
import os
import random
import re
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import sys
from analyzer import Analyzer
from common import get_option, make_connection
from lang_wrap import init_lang_recog
from show_case import ShowCase
from stem_mixin import StemMixin
from stop_util import load_stop_words
from token_util import tokenize

class Processor(ShowCase, StemMixin):
    def __init__(self, cur, stop_words):
        ShowCase.__init__(self, cur)
        StemMixin.__init__(self)
        random.seed()
        self.stop_words = stop_words
        self.cluster_count = int(get_option("cluster_count", "128"))
        self.lang_recog = init_lang_recog()
        self.sample_size = int(get_option("heatmap_sample_size", "100"))
        self.url2doc = {}
        self.topics = []
        self.matrix = None

    def load_item(self, et):
        if self.is_redirected(et['url']):
            return

        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            self.extend_date(et)
            txt = self.reconstitute(et)
            self.url2doc[et['url']] = txt

    def process(self):
        cv = CountVectorizer(max_df=0.95, min_df=2, analyzer=Analyzer(self.stop_words))
        docs = [ doc for url, doc in sorted(self.url2doc.items(), key=lambda p: p[0]) ]
        if not len(docs): # seen w/ incorrect download config
            raise Exception("No input documents found - check filtering conditions in load_item.")

        df = cv.fit_transform(docs)
        words = cv.get_feature_names()
        lda = LatentDirichletAllocation(n_components=self.cluster_count, learning_method='online')
        lda.fit(df)

        for index, topic in enumerate(lda.components_):
            top = "\n".join([words[i] for i in topic.argsort()[-4:]])
            self.topics.append(top)

        self.matrix = lda.transform(df)

    def sample(self):
        url2doc = {}
        urls = self.get_urls()
        matrix = []
        l = len(urls)
        if l > self.sample_size:
            curve = [np.max(self.matrix[i]) for i in range(l)]
            curve.sort(reverse=True)
            threshold = curve[self.sample_size]
            for i in range(l):
                mx = np.max(self.matrix[i])
                if mx > threshold:
                    url = urls[i]
                    url2doc[url] = self.url2doc[url]
                    matrix.append(self.matrix[i])

        self.url2doc = url2doc
        self.matrix = matrix

    def dump_meta(self, output_path):
        meta = {
            'rowDesc': self.get_urls(),
            'colDesc': self.topics,
            'dateExtent': self.make_date_extent(),
            'maxValue': self.get_max_value()
        }

        with open(output_path, 'w') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

    def dump_content(self, output_path):
        with open(output_path, 'w') as f:
            writer = csv.writer(f, delimiter=",")
            headings = [ "doc", "topic", "value" ]
            writer.writerow(headings)

            for i in range(len(self.url2doc)):
                for j in range(len(self.topics)):
                    row = ( i, j, self.matrix[i][j] )
                    writer.writerow(row)

    def get_urls(self):
        return sorted(self.url2doc.keys())

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]

    def get_max_value(self):
        return np.max(self.matrix)


def main():
    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur, stop_words)
            processor.run()
            processor.process()
            processor.sample()

            json_target = get_option("heatmap_meta_data", "heatmap.json")
            processor.dump_meta(json_target)
            csv_target = get_option("heatmap_data", "heatmap.csv")
            processor.dump_content(csv_target)

if __name__ == "__main__":
    main()
