#!/usr/bin/python3

import csv
import json
import numpy as np
import random
import re
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import sys
from common import get_option, make_connection
from lang_wrap import init_lang_recog
from show_case import ShowCase
from token_util import tokenize, tokenize_persons, retokenize

class Processor(ShowCase):
    def __init__(self, cur, stop_words):
        ShowCase.__init__(self, cur)
        random.seed()
        self.stop_words = stop_words
        self.cluster_count = int(get_option("cluster_count", "128"))
        self.tokenize = tokenize_persons if get_option("cluster_persons", "") else self.tokenize_all
        self.lang_recog = init_lang_recog()
        self.sample_threshold = float(get_option("heatmap_sample_threshold", "0.01"))
        self.docs = []
        self.urls = []
        self.topics = []
        self.matrix = None

    def load_item(self, et):
        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            self.extend_date(et)
            long_lst = self.tokenize(et['text'])
            txt = " ".join(long_lst)
            self.docs.append(txt)
            self.urls.append(et['url'])

    @staticmethod
    def tokenize_all(txt):
        return tokenize(txt, True)

    def process(self):
        cv = CountVectorizer(max_df=0.95, min_df=2, tokenizer=retokenize, stop_words=self.stop_words)
        df = cv.fit_transform(self.docs)
        words = cv.get_feature_names()
        lda = LatentDirichletAllocation(n_components=self.cluster_count)
        lda.fit(df)

        for index, topic in enumerate(lda.components_):
            top = " ".join([words[i] for i in topic.argsort()[-4:]])
            self.topics.append(top)

        self.matrix = lda.transform(df)

    def sample(self):
        urls = []
        matrix = []
        for i in range(len(self.urls)):
            r = random.random()
            if r < self.sample_threshold:
                urls.append(self.urls[i])
                matrix.append(self.matrix[i])

        self.urls = urls
        self.matrix = matrix

    def dump_meta(self, output_path):
        meta = {
            'rowDesc': self.urls,
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

            for i in range(len(self.urls)):
                for j in range(len(self.topics)):
                    row = ( i, j, self.matrix[i][j] )
                    writer.writerow(row)

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]

    def get_max_value(self):
        return np.max(self.matrix)


def main():
    print("loading stop words...", file=sys.stderr)
    stop_list_file = get_option("stop_list_file", "stoplist.txt")
    stop_words = []
    with open(stop_list_file) as f:
        for ln in f:
            lst = ln.split()
            if lst:
                stop_words.append(lst[0])

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
