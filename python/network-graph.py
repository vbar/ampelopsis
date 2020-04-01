#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import networkx as nx
import re
import sys
from common import get_option, make_connection
from merchandise import ActivitySelector
from show_case import ShowCase
from url_heads import town_url_head

class RefNet(ShowCase):
    def __init__(self, cur, white_hamlet_names):
        ShowCase.__init__(self, cur)
        self.hamlet2town = {} # str -> [ str ]
        self.town2presentation = {} # str -> str
        self.town2color = {} # str -> str color (6 chars, w/o #)
        self.init_mapping(white_hamlet_names)
        self.next_shade = 'A'
        self.pair2node = {} # (str town name, bool from?) -> int node index
        self.node2pair = {} # int node index -> (str town_name, str presentation name)
        self.nick_rx = re.compile('@([-\\w]+)')
        self.ref_map = {} # (int, int) edge -> int weight

    def init_mapping(self, white_hamlet_names):
        white_set = set(white_hamlet_names)
        self.cur.execute("""select hamlet_name, town_name, presentation_name, color
from vn_record
join vn_identity_hamlet on record_id=vn_record.id
left join vn_party on vn_party.id=party_id
order by hamlet_name, town_name""")
        rows = self.cur.fetchall()
        for row in rows:
            hamlet_name, town_name, present_name, color = row
            if hamlet_name in white_set:
                self.town2presentation[town_name] = present_name
                lst = self.hamlet2town.get(hamlet_name)
                if lst is None:
                    self.hamlet2town[hamlet_name] = [ town_name ]
                else:
                    lst.append(town_name)

                if color:
                    self.town2color[town_name] = color

    def dump(self):
        ebunch = [(edge[0], edge[1], weight) for edge, weight in self.ref_map.items()]
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
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
                edge_set = set()
                txt = et.get('text')
                for m in self.nick_rx.finditer(txt):
                    target_name = m.group(1)
                    if target_name in self.town2presentation:
                        first = True
                        for town_name in town_names:
                            if town_name != target_name:
                                if first:
                                    # introduce only those nodes which
                                    # will have edges - D3 sankey
                                    # crashes on a sparse sequence...
                                    town_node = self.introduce_node(town_name, True)
                                    target_node = self.introduce_node(target_name, False)
                                    edge_set.add((town_node, target_node))
                                    first = False
                                else:
                                    self.ensure_node(town_name, town_node)

                for edge in edge_set:
                    weight = self.ref_map.get(edge, 0)
                    self.ref_map[edge] = weight + 1

    def enrich(self, gd):
        for gn in gd['nodes']:
            node_idx = gn['node']
            town_name, present_name = self.node2pair[node_idx]
            gn['name'] = present_name
            gn['ext_url'] = "%s/%s" % (town_url_head, town_name)
            gn['color'] = self.introduce_color(town_name)

        # D3 sankey calls weight "value"...
        for lnk in gd['links']:
            weight = lnk.pop('weight')
            lnk['value'] = weight

    def introduce_node(self, town_name, from_flag):
        node_idx = self.pair2node.get((town_name, from_flag))
        if node_idx is None:
            node_idx = len(self.pair2node) # 0-based
            self.pair2node[(town_name, from_flag)] = node_idx
            present_name = self.town2presentation.get(town_name, town_name)
            self.node2pair[node_idx] = (town_name, present_name)

        return node_idx

    def ensure_node(self, town_name, town_node):
        node_idx = self.pair2node.get((town_name, False))
        if node_idx is None:
            self.pair2node[(town_name, False)] = town_node
        elif node_idx != town_node:
            raise Exception("Multiple persons for "  + town_name)

    def introduce_color(self, town_name):
        color = self.town2color.get(town_name)
        if not color:
            color = self.next_shade * 6
            self.next_shade = chr(ord(self.next_shade) + 1)
            if self.next_shade == 'D': # independents have that
                self.next_shade = 'A'

            self.town2color[town_name] = color

        return '#' + color


def get_selected_contributors(cur):
    top_limit = int(get_option("top_contributor_limit", "12"))
    selector = ActivitySelector(cur)
    try:
        selector.run()
        return selector.get_selected_contributors(top_limit)
    finally:
        selector.close()


def dump_network(cur, selected_contributors):
    ref_net = RefNet(cur, selected_contributors)
    try:
        ref_net.run()
        ref_net.dump()
    finally:
        ref_net.close()


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            selected_contributors = get_selected_contributors(cur)
            dump_network(cur, selected_contributors)


if __name__ == "__main__":
    main()
