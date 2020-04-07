#!/usr/bin/python3

# requires database filled by running condensate.py

import collections
from dateutil.parser import parse
import json
import networkx as nx
import re
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase
from url_heads import town_url_head

TagOcc = collections.namedtuple('TagOcc', 'tag hamlet_name occ_date')

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.tag_rx = re.compile('#([-\\w]+)')
        self.ref_map = {} # (int, int) edge -> int weight
        self.tag_line = [] # of TagOcc

    def dump(self):
        self.lazy_ref_map()

        ebunch = [(edge[0], edge[1], weight) for edge, weight in self.ref_map.items()]
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def dump_meta(self, output_path):
        self.lazy_ref_map()

        meta = []
        n = len(self.pair2node)
        for i in range(n):
            variant = self.node2variant[i]

            if type(variant) is str:
                name = self.person_map[variant]
            else:
                name = self.party_map[variant]

            color = self.introduce_color(variant)

            meta.append({'name': name, 'color': color})

        with open(output_path, 'w') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

    def dump_matrix(self, output_path):
        self.lazy_ref_map()

        matrix = []
        n = len(self.pair2node)
        for i in range(n):
            matrix.append([ 0 ] * n)

        for edge, weight in self.ref_map.items():
            row = matrix[edge[0]]
            row[edge[1]] = weight

        with open(output_path, 'w') as f:
            json.dump(matrix, f, indent=2, ensure_ascii=False)

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            hamlet_name = et['osobaid']
            occ_date = parse(et['datum'])
            txt = et['text']
            for m in self.tag_rx.finditer(txt):
                self.tag_line.append(TagOcc(tag=m.group(1), hamlet_name=hamlet_name, occ_date=occ_date))

    def lazy_ref_map(self):
        if len(self.ref_map):
            return

        tag_line = sorted(self.tag_line, key=lambda tgo: tgo.occ_date)
        topics = {} # tag => set of variant
        for tgo in tag_line:
            variant = self.get_variant(tgo.hamlet_name)
            if variant is not None:
                top = topics.get(tgo.tag)
                if top is None:
                    originator = set()
                    originator.add(variant)
                    topics[tgo.tag] = originator
                elif variant not in top:
                    target_node = self.introduce_node(variant, False)
                    for prev_variant in top:
                        source_node = self.introduce_node(prev_variant, self.distinguish)
                        edge = (source_node, target_node)
                        weight = self.ref_map.get(edge, 0)
                        self.ref_map[edge] = weight + 1

                    top.add(variant)


def main():
    ca = ConfigArgs()
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            ref_net = RefNet(cur, ca.distinguish, parties)
            try:
                ref_net.run()
                if ca.distinguish:
                    ref_net.dump()
                else:
                    if ca.meta:
                        ref_net.dump_meta(ca.meta)

                    if ca.matrix:
                        ref_net.dump_matrix(ca.matrix)
            finally:
                ref_net.close()


if __name__ == "__main__":
    main()
