#!/usr/bin/python3

# requires download with funnel_links set to 3 and database filled by
# running condensate.py

import sys
from common import get_option, make_connection
from distance_args import ConfigArgs
from pinhole_base import PinholeBase
from trail_mixin import TrailMixin


def compute_set_score(a, b):
    union = a | b
    den = len(union)
    if not den:
        return None

    intersection = a & b
    nom = len(intersection)
    return nom / den


class Processor(PinholeBase, TrailMixin):
    def __init__(self, cur):
        PinholeBase.__init__(self, cur, False, '*')
        TrailMixin.__init__(self)
        self.silent = True
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))
        self.hamlet2followers = {}

    def load_item(self, et):
        hamlet_name = et['osobaid']
        town_name = self.hamlet2town.get(hamlet_name)
        if town_name:
            self.extend_date(et)
            if hamlet_name not in self.hamlet2followers:
                self.hamlet2followers[hamlet_name] = self.make_followers_set(town_name)

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        for gn in gd['nodes']:
            node_idx = gn['node']
            hamlet_name = self.node2variant[node_idx]
            following = self.hamlet2followers[hamlet_name]
            gn['doc_count'] = len(following)

    def process(self):
        persons = []
        vector = []
        for hamlet_name, fset in sorted(self.hamlet2followers.items(), key=lambda p: (-1 * len(p[1]), p[0])):
            persons.append(hamlet_name)
            vector.append(fset)

        l = len(vector)
        for i in range(l):
            for j in range(i + 1, l):
                print("measuring similarity between %s and %s..." % (persons[i], persons[j]), file=sys.stderr)
                sim = compute_set_score(vector[i], vector[j])
                if (sim is not None) and (sim > self.link_threshold):
                    # hamlet name is-a variant
                    low_node = self.introduce_node(persons[i], False)
                    high_node = self.introduce_node(persons[j], False)
                    edge = (low_node, high_node)
                    self.ref_map[edge] = 1 / sim


def main():
    ca = ConfigArgs()
    with make_connection() as conn:
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


if __name__ == "__main__":
    main()
