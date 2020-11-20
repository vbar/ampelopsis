#!/usr/bin/python3

import json
import numpy as np
import random
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
        self.visible_clusters = int(get_option("visible_clusters", "128"))
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
            if txt:
                self.url2doc[et['url']] = txt

    def process(self):
        print("computing topics...", file=sys.stderr)

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
        urls = self.get_urls()
        l = len(urls)
        if l > self.sample_size:
            print("sampling %d of %d URLs..." % (self.sample_size, l), file=sys.stderr)
            url2doc = {}
            matrix = []
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

        l = len(self.topics)
        if self.visible_clusters < l:
            print("sampling %d of %d topics..." % (self.visible_clusters, l), file=sys.stderr)
            supplementary_threshold = float(self.visible_clusters) / float(l)
            wide = np.array(self.matrix)
            assert l == wide.shape[1]
            narrow = None
            topics = []
            curve = [np.max(wide[:, i]) for i in range(l)]
            curve.sort(reverse=True)
            threshold = curve[self.visible_clusters]
            for i in range(l):
                col = wide[:, i:i+1]
                mx = np.max(col)
                if (mx > threshold) or ((mx == threshold) and (random.random() < supplementary_threshold)):
                    if narrow is None:
                        narrow = np.array(col, copy=True)
                    else:
                        narrow = np.concatenate((narrow, col), axis=1)

                    topics.append(self.topics[i])
                    if len(topics) >= self.visible_clusters:
                        break

            assert narrow.shape[1] == len(topics)
            self.matrix = narrow.tolist()
            self.topics = topics

        print("%d documents with %d topics after sampling" % (len(self.matrix), len(self.topics)), file=sys.stderr)

    def dump(self):
        meta = {
            'rowDesc': self.get_urls(),
            'colDesc': self.topics,
            'table': self.get_table(),
            'dateExtent': self.make_date_extent(),
            'maxValue': self.get_max_value()
        }

        print(json.dumps(meta, indent=2))

    def get_table(self):
        table = []
        for i in range(len(self.url2doc)):
            for j in range(len(self.topics)):
                row = ( i, j, self.matrix[i][j] )
                table.append(row)

        return table

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
            processor.dump()

if __name__ == "__main__":
    main()
