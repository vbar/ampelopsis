#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import networkx as nx
import numpy as np
import sys
from common import get_option, make_connection
from dirichlet_base import DirichletBase
from party_mixin import PartyMixin
from stop_util import load_stop_words

class Payload:
    def __init__(self, prob, argmax):
        self.total = prob
        self.maximum = prob
        self.argmax = argmax

    def update(self, prob, argmax):
        self.total += prob

        if prob > self.maximum:
            self.maximum = prob
            self.argmax = argmax


class Processor(DirichletBase, PartyMixin):
    def __init__(self, cur, stop_words):
        DirichletBase.__init__(self, cur, stop_words)
        PartyMixin.__init__(self)
        self.topic_match_threshold = float(get_option("topic_match_threshold", "0.67"))
        self.url2party = {}
        self.average = None
        self.joint = {} # (int, int) edge -> float
        self.party_axis = None

    def load_doc(self, et):
        txt = self.reconstitute(et)
        if txt:
            self.extend_date(et)
            url = self.get_circuit_url(et['url'])
            self.url2doc[url] = txt

            self.url2party[url] = self.hamlet2party.get(et['osobaid'], 0)

    def dump(self):
        self.compute_joint()

        ebunch = ((edge[0], edge[1], (self.average[edge[0]] * self.average[edge[1]]) / jp) for edge, jp in self.joint.items())
        graph = nx.Graph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def compute_joint(self):
        self.average = self.matrix.mean(axis=1)

        dn = len(self.url2doc.keys())
        assert self.matrix.shape[0] == dn
        tn = self.cluster_count
        assert self.matrix.shape[1] == tn
        i = 0
        while i < tn:
            j = i + 1
            while j < tn:
                prod = np.inner(self.matrix[:, i], self.matrix[:, j])
                self.joint[(i, j)] = prod / dn
                j += 1
            i += 1

    def enrich(self, gd):
        significant = np.count_nonzero(self.matrix > self.topic_match_threshold, axis=1)
        urls = self.get_urls()

        self.party_axis = []
        party2total = {}
        for i, url in enumerate(urls):
            party_id = self.url2party[url]
            self.party_axis.append(party_id)

            sub_total = np.sum(self.matrix[i])
            old_total = party2total.get(party_id, 0)
            party2total[party_id] = old_total + sub_total

        parties = [ p[0] for p in sorted(party2total.items(), key=lambda q: (-1 * q[1], q[0])) ]
        party_index = {}
        names = []
        colors = []
        for i, party_id in enumerate(parties):
            party_index[party_id] = i
            names.append(self.party_map.get(party_id, "???"))
            colors.append('#' + self.party2color.get(party_id, 'fff'))

        gd['names'] = names
        gd['colors'] = colors

        for gn in gd['nodes']:
            node_idx = gn['node']
            gn['name'] = self.topics[node_idx]
            gn['large'] = bool(significant[node_idx])

            party2payload = self.get_proportions(node_idx)
            total_seq = sorted(party2payload.items(), key=lambda p: (-1 * p[1].total, p[0]))
            gn['proportions'] = [ [ party_index[p[0]], p[1].total, urls[p[1].argmax] ] for p in total_seq ]

        for lnk in gd['links']:
            weight = lnk.pop('weight')
            lnk['value'] = weight

        if self.mindate and self.maxdate:
            gd['dateExtent'] = self.make_date_extent()

    def get_proportions(self, topic_idx):
        party2payload = {}
        for i in range(len(self.url2doc.keys())):
            party_id = self.party_axis[i]
            prob = self.matrix[i, topic_idx]

            payload = party2payload.get(party_id)
            if payload is None:
                party2payload[party_id] = Payload(prob, i)
            else:
                payload.update(prob, i)

        return party2payload

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]


def main():
    stop_words = load_stop_words()
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur, stop_words)
            processor.run()
            processor.process()
            processor.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
