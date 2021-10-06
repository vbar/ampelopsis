#!/usr/bin/python3

# requires database filled by running condensate.py

import sys
from common import get_option, make_connection
from distance_args import ConfigArgs
from jaccard_util import jaccard_score
from pinhole_base import PinholeBase
from timeline_mixin import TimelineMixin

def by_reverse_value_sum(p):
    return (-1 * sum(p[1]), p[0])


class Processor(PinholeBase, TimelineMixin):
    def __init__(self, cur):
        PinholeBase.__init__(self, cur, False, '*')
        puff = int(get_option("event_distance_puff", "5"))
        TimelineMixin.__init__(self, 'minutes', puff) # key is hamlet name
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))

    def load_item(self, et):
        if self.is_redirected(et['url']):
            return

        dt = self.extend_date(et)
        hamlet_name = et['osobaid']
        self.add_sample(hamlet_name, dt)

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        value_series = self.get_value_series()
        for gn in gd['nodes']:
            node_idx = gn['node']
            hamlet_name = self.node2variant[node_idx]
            series = value_series[hamlet_name]
            gn['doc_count'] = sum(series) / (1 + 2 * self.puff)

    def process(self):
        value_series = self.get_value_series()
        persons = []
        matrix = []
        for hamlet_name, series in sorted(value_series.items(), key=by_reverse_value_sum):
            persons.append(hamlet_name)
            matrix.append(series)

        l = len(matrix)
        for i in range(l):
            for j in range(i + 1, l):
                print("measuring similarity between %s and %s..." % (persons[i], persons[j]), file=sys.stderr)
                sim = jaccard_score(matrix[i], matrix[j])
                if (sim is not None) and (sim > self.link_threshold):
                    # hamlet name is-a variant
                    low_node = self.introduce_node(persons[i], False)
                    high_node = self.introduce_node(persons[j], False)
                    edge = (low_node, high_node)
                    self.ref_map[edge] = 1 / sim


def main():
    ca = ConfigArgs()
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.process()
                processor.dump_undirected()
                if ca.histogram:
                    processor.dump_distance_histogram(ca.histogram)
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
