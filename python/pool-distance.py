#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import collections
import json
import networkx as nx
import sys
from common import get_option, make_connection
from pinhole_base import PinholeBase

Occurence = collections.namedtuple('Occurence', 'hamlet_name date_time')

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
        self.known = {} # int url id -> Occurence
        self.expected = {} # int url id -> set of Occurence
        self.terrain = {} # source hamlet name -> target hamlet name -> count
        self.hamlet2count = {}

    def load_item(self, et):
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        target_hamlet_name = et['osobaid']
        target_date = self.extend_date(et)
        target_occ = Occurence(target_hamlet_name, target_date)
        self.add_known(url_id, target_occ)

        self.cur.execute("""select f2.id
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.id=%s""", (url_id,))
        rows = self.cur.fetchall()
        for row in rows:
            source_url_id = row[0]
            source_occ = self.known.get(source_url_id)
            if source_occ is None:
                targets = self.expected.get(source_url_id)
                if targets is None:
                    self.expected[source_url_id] = set((target_occ,))
                else:
                    targets.add(target_occ)
            else:
                self.add_redir(source_occ, target_occ)

    def dump_final_state(self):
        for url_id, targets in sorted(self.expected.items()):
            url = self.get_url(url_id)
            print(url, file=sys.stderr)
            for target_occ in sorted(targets):
                print("\t" + target_occ.hamlet_name, file=sys.stderr)

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

    def add_known(self, url_id, occ):
        if url_id in self.known:
            return

        self.known[url_id] = occ

        targets = self.expected.get(url_id)
        if not targets:
            return

        for target_occ in targets:
            self.add_redir(occ, target_occ)

        del self.expected[url_id]

    def add_redir(self, source_occ, target_occ):
        if source_occ.date_time == target_occ.date_time:
            self.add_link(source_occ.hamlet_name, target_occ.hamlet_name)

    def add_link(self, source_name, target_name):
        target2count = self.terrain.get(source_name)
        if target2count is None:
            self.terrain[source_name] = { target_name: 1 }
        else:
            cnt = target2count.get(target_name, 0)
            target2count[target_name] = cnt + 1

    def get_target_keys(self):
        target_keys = set()
        for hamlet_name, target2count in self.terrain.items():
            for town_name in target2count.keys():
                target_keys.add(town_name)

        return sorted(target_keys)

    def get_weight(self, source_name, target_name):
        target2count = self.terrain.get(source_name)
        if target2count is None:
            return 0
        else:
            return target2count.get(target_name, 0)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.process()
                processor.dump_final_state()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
