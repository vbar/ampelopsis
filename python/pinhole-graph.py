#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import networkx as nx
import re
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase
from url_heads import town_url_head

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.nick_rx = re.compile('@([-\\w]+)')
        self.ref_map = {} # (int, int) edge -> int weight

    def dump(self):
        ebunch = [(edge[0], edge[1], weight) for edge, weight in self.ref_map.items()]
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def dump_meta(self, output_path):
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
        matrix = []
        n = len(self.pair2node)
        for i in range(n):
            matrix.append([ 0 ] * n)

        for edge, weight in self.ref_map.items():
            row = matrix[edge[0]]
            row[edge[1]] = weight

        with open(output_path, 'w') as f:
            json.dump(matrix, f, indent=2, ensure_ascii=False)

    def load_item(self, et):
        self.extend_date(et)
        source_hamlet_name = et.get('osobaid')
        source_variant = self.get_variant(source_hamlet_name)
        if source_variant:
            self.do_load_item(et, source_hamlet_name, source_variant)

    def do_load_item(self, et, source_hamlet_name, source_variant):
        edge_set = set()
        txt = et.get('text')
        for m in self.nick_rx.finditer(txt):
            target_town_name = m.group(1)
            target_hamlet_name = self.town2hamlet.get(target_town_name)
            if target_hamlet_name and (target_hamlet_name != source_hamlet_name):
                target_variant = self.get_variant(target_hamlet_name)
                if target_variant:
                    source_node = self.introduce_node(source_variant, self.distinguish)
                    target_node = self.introduce_node(target_variant, False)
                    edge_set.add((source_node, target_node))

        for edge in edge_set:
            weight = self.ref_map.get(edge, 0)
            self.ref_map[edge] = weight + 1


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
