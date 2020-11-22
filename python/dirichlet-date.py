#!/usr/bin/python3

import json
import numpy as np
import sys
from common import make_connection
from dirichlet_base import DirichletBase
from stop_util import load_stop_words

class Processor(DirichletBase):
    def __init__(self, cur, stop_words):
        DirichletBase.__init__(self, cur, stop_words)
        self.url2date = {}
        self.evolution = {} # rounded datetime -> array of float activity (indexed by topic)

    def load_doc(self, et):
        txt = self.reconstitute(et)
        if txt:
            url = et['url']
            dt = self.extend_date(et)
            self.url2doc[url] = txt
            self.url2date[url] = dt

    def postprocess(self):
        print("summing topic presence in time...", file=sys.stderr)

        topic_count = len(self.topics)
        urls = self.get_urls()
        for i in range(len(urls)):
            url = urls[i]
            dt = self.url2date[url]
            rdt = dt.replace(microsecond=0, second=0, minute=0)
            instant = self.evolution.get(rdt)
            if instant is None:
                instant = [0.0] * topic_count
                self.evolution[rdt] = instant

            for j in range(topic_count):
                instant[j] += self.matrix[i][j]

        # memory optimization
        self.matrix = None

    def dump(self):
        meta = {
            'rowDesc': self.topics,
            'colDesc': self.get_dates(),
            'table': self.get_table(),
            'maxValue': self.get_max_value()
        }

        print(json.dumps(meta, indent=2, ensure_ascii=False))

    def get_dates(self):
        dates = []
        for dt, instant in sorted(self.evolution.items(), key=lambda p: p[0]):
            dates.append(dt.isoformat())

        return dates

    def get_table(self):
        table = []
        topic_count = len(self.topics)
        di = 0
        for dt, instant in sorted(self.evolution.items(), key=lambda p: p[0]):
            for j in range(topic_count):
                row = ( di, j, instant[j] )
                table.append(row)

            di += 1

        return table

    def get_max_value(self):
        mx = 0.0
        for dt, instant in self.evolution.items():
            m = max(instant)
            if m > mx:
                mx = m

        return mx


def main():
    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur, stop_words)
            processor.run()
            processor.process()
            processor.sample_topics()
            processor.postprocess()
            processor.dump()

if __name__ == "__main__":
    main()
