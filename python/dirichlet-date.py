#!/usr/bin/python3

import collections
import json
import numpy as np
import sys
from common import get_option, make_connection
from dirichlet_base import DirichletBase
from stop_util import load_stop_words

SampleOcc = collections.namedtuple('SampleOcc', 'url topic_prob')

class Processor(DirichletBase):
    def __init__(self, cur, stop_words):
        DirichletBase.__init__(self, cur, stop_words)
        self.cell_sample_size = int(get_option('datemap_sample_size', "3"))
        if self.cell_sample_size < 1:
            raise Exception("datemap_sample_size must be positive")

        self.url2date = {}
        self.evolution = {} # rounded datetime -> array of float activity (indexed by topic)
        self.cell2samples = {} # rounded datetime, topic index pair -> array of at most cell_sample_size SampleOcc sorted by float SampleOcc.topic_prob, descending

    def load_doc(self, et):
        txt = self.reconstitute(et)
        if txt:
            url = self.get_circuit_url(et['url'])
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
                tp = self.matrix[i][j]
                instant[j] += tp

                samples = self.cell2samples.get((rdt, j))
                if samples is None:
                    self.cell2samples[(rdt, j)] = [ SampleOcc(url=url, topic_prob=tp) ]
                else:
                    sort_needed = True
                    if len(samples) < self.cell_sample_size:
                        samples.append(SampleOcc(url=url, topic_prob=tp))
                    else:
                        worst_occ = samples[-1]
                        if worst_occ.topic_prob < tp:
                            samples[-1] = SampleOcc(url=url, topic_prob=tp)
                        else:
                            sort_needed = False # no change

                    if sort_needed:
                        self.cell2samples[(rdt, j)] = sorted(samples, key=lambda so: -1 * so.topic_prob)

        # memory optimization
        self.matrix = None

    def dump(self):
        meta = {
            'rowDesc': self.topics,
            'colDesc': self.get_dates(),
            'table': self.get_table(),
            'samples': self.get_samples(),
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

    def get_samples(self):
        matrix = []
        topic_count = len(self.topics)
        for dt, instant in sorted(self.evolution.items(), key=lambda p: p[0]):
            row = []
            for j in range(topic_count):
                samples = self.cell2samples.get((dt, j))
                cell = [ s.url for s in samples ] if samples is not None else []
                row.append(cell)

            matrix.append(row)

        return matrix

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
