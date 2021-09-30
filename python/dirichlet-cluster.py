#!/usr/bin/python3

import json
import numpy as np
import sys
from common import get_option, make_connection
from dirichlet_base import DirichletBase
from party_mixin import PartyMixin
from stop_util import load_stop_words

class Processor(DirichletBase, PartyMixin):
    def __init__(self, cur, stop_words):
        DirichletBase.__init__(self, cur, stop_words)
        PartyMixin.__init__(self)
        self.sample_size = int(get_option("heatmap_sample_size", "100"))
        self.url2color = {}

    def load_doc(self, et):
        txt = self.reconstitute(et)
        if txt:
            self.extend_date(et)
            url = self.get_circuit_url(et)
            self.url2doc[url] = txt

            party_id = self.hamlet2party.get(et['osobaid'], 0)
            self.url2color[url] = self.party2color.get(party_id, 'f0027f')

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

        self.sample_topics()

        print("%d documents with %d topics after sampling" % (len(self.matrix), len(self.topics)), file=sys.stderr)

    def dump(self):
        meta = {
            'rowDesc': self.get_urls(),
            'rowColor': self.get_colors(),
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

    def get_colors(self):
        colors = []
        for url in self.get_urls():
            colors.append('#' + self.url2color[url])

        return colors

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
