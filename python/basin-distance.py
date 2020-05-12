#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import json
import networkx as nx
import sys
from urllib.parse import urlparse
from common import get_option, make_connection
from funnel_parser import status_rx
from pinhole_base import PinholeBase

def weighted_jaccard_score(a, b):
    nom = 0
    den = 0
    l = len(a)
    assert l == len(b)
    for i in range(l):
        if a[i]:
            if b[i]:
                nom += a[i]
                nom += b[i]

            den += a[i]
            den += b[i]
        else:
            den += b[i]

    if not den:
        return None

    return nom / den

class Processor(PinholeBase):
    def __init__(self, cur):
        PinholeBase.__init__(self, cur, False, '*')
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))
        self.terrain = {} # source hamlet name -> target town name -> count
        self.hamlet2count = {}

    def load_item(self, et):
        self.extend_date(et)
        source_hamlet_name = et['osobaid']
        town_url = et.get('url')
        if town_url:
            self.cur.execute("""select f2.url
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.url=%s""", (town_url,))
            rows = self.cur.fetchall()
            for row in rows:
                pr = urlparse(row[0])
                m = status_rx.match(pr.path)
                if m:
                    self.add_link(source_hamlet_name, m.group(1))

    def dump(self):
        ebunch = [(edge[0], edge[1], dist) for edge, dist in self.ref_map.items()]
        graph = nx.Graph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        for gn in gd['nodes']:
            node_idx = gn['node']
            hamlet_name = self.node2variant[node_idx]
            gn['doc_count'] = self.hamlet2count[hamlet_name]

    def process(self):
        from_labels = sorted(self.terrain.keys())
        to_labels = self.get_target_keys()

        matrix = []
        for source_hamlet_name in from_labels:
            row = []
            for target_town_name in to_labels:
                row.append(self.get_weight(source_hamlet_name, target_town_name))

            matrix.append(row)
            self.hamlet2count[source_hamlet_name] = sum(row)

        l = len(matrix)
        for i in range(l):
            for j in range(i + 1, l):
                print("measuring similarity between %s and %s..." % (from_labels[i], from_labels[j]), file=sys.stderr)
                sim = weighted_jaccard_score(matrix[i], matrix[j])
                if (sim is not None) and (sim > self.link_threshold):
                    # hamlet name is-a variant
                    low_node = self.introduce_node(from_labels[i], False)
                    high_node = self.introduce_node(from_labels[j], False)
                    edge = (low_node, high_node)
                    self.ref_map[edge] = 1 / sim

    def add_link(self, source_hamlet_name, target_town_name):
        target2count = self.terrain.get(source_hamlet_name)
        if target2count is None:
            self.terrain[source_hamlet_name] = { target_town_name: 1 }
        else:
            cnt = target2count.get(target_town_name, 0)
            target2count[target_town_name] = cnt + 1

    def get_target_keys(self):
        target_keys = set()
        for hamlet_name, target2count in self.terrain.items():
            for town_name in target2count.keys():
                target_keys.add(town_name)

        return sorted(target_keys)

    def get_weight(self, source_hamlet_name, target_town_name):
        target2count = self.terrain.get(source_hamlet_name)
        if target2count is None:
            return 0
        else:
            return target2count.get(target_town_name, 0)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.process()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
