#!/usr/bin/python3

import json
import matplotlib.pyplot as plt
import networkx as nx
import re
import sys
from common import make_connection
from show_case import ShowCase

class RefNet(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.town_set = set()
        self.hamlet2town = {} # str -> [ str ]
        self.init_mapping()
        self.nick_rx = re.compile('@([-\\w]+)')
        self.ref_map = {} # (str, str) edge -> int weight

    def init_mapping(self):
        self.cur.execute("""select hamlet_name, town_name
from vn_identity_hamlet
order by hamlet_name, town_name""")
        rows = self.cur.fetchall()
        for row in rows:
            hamlet_name, town_name = row
            self.town_set.add(town_name)
            lst = self.hamlet2town.get(hamlet_name)
            if lst is None:
                self.hamlet2town[hamlet_name] = [ town_name ]
            else:
                lst.append(town_name)

    def dump(self):
        ebunch = [(edge[0], edge[1], weight) for edge, weight in self.ref_map.items()]
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph)
        print(json.dumps(gd, indent=2))
        # giant = max(nx.strongly_connected_components(graph), key=len)

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            hamlet_name = et.get('osobaid')
            town_names = self.hamlet2town.get(hamlet_name)
            if town_names:
                # Different nicks of the same person (both source and
                # destination) are counted separately - is that
                # correct?
                edge_set = set()
                txt = et.get('text')
                for m in self.nick_rx.finditer(txt):
                    target_name = m.group(1)
                    if target_name in self.town_set:
                        for town_name in town_names:
                            if town_name != target_name:
                                edge_set.add((town_name, target_name))

                for edge in edge_set:
                    weight = self.ref_map.get(edge, 0)
                    self.ref_map[edge] = weight + 1


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            rn = RefNet(cur)
            try:
                rn.run()
                rn.dump()
            finally:
                rn.close()


if __name__ == "__main__":
    main()
