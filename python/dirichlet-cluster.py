#!/usr/bin/python3

import json
import numpy as np
import random
import sys
from common import get_option, make_connection
from dirichlet_base import DirichletBase
from stop_util import load_stop_words

class Processor(DirichletBase):
    def __init__(self, cur, stop_words):
        DirichletBase.__init__(self, cur, stop_words)
        random.seed()
        self.visible_clusters = int(get_option("visible_clusters", "128"))
        self.sample_size = int(get_option("heatmap_sample_size", "100"))

    def load_doc(self, et):
        txt = self.reconstitute(et)
        if txt:
            self.extend_date(et)
            self.url2doc[et['url']] = txt

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
