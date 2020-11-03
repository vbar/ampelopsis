import json
import networkx as nx
from person_party_mixin import PersonPartyMixin
from show_case import ShowCase
from url_heads import town_url_head

class PinholeBase(ShowCase, PersonPartyMixin):
    def __init__(self, cur, distinguish, deconstructed):
        ShowCase.__init__(self, cur)
        PersonPartyMixin.__init__(self, deconstructed)
        self.distinguish = distinguish
        self.pair2node = {} # (str hamlet name / int party id, bool from?) -> int node index
        self.node2variant = {} # int node index -> str hamlet name / int party id
        self.ref_map = {} # (int, int) edge -> int weight

    def dump_standard(self):
        self.lazy_ref_map()

        ebunch = [(edge[0], edge[1], weight) for edge, weight in self.ref_map.items()]
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def dump_chord(self, output_path):
        self.lazy_ref_map()

        custom = {
            'matrix': self.make_matrix(),
            'desc': self.make_desc(),
            'matrixDesc': self.make_matrix_desc()
        }

        if self.mindate and self.maxdate:
            custom['dateExtent'] = self.make_date_extent()

        with open(output_path, 'w') as f:
            json.dump(custom, f, indent=2, ensure_ascii=False)

    def dump_undirected(self):
        self.lazy_ref_map()

        ebunch = [(edge[0], edge[1], 1 / weight) for edge, weight in self.ref_map.items()]
        graph = nx.Graph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def dump_distance_histogram(self, output_path):
        self.lazy_ref_map()

        same = []
        other = []
        for edge, weight in sorted(self.ref_map.items(), key=lambda p: -1 * p[1]):
            party_ids = []
            for node in edge:
                variant = self.node2variant[node]
                party_ids.append(self.get_party_id(variant))

            if party_ids[0] == party_ids[1]:
                same.append(weight)
            else:
                other.append(weight)

        custom = {
            'same': same,
            'other': other
        }

        if self.mindate and self.maxdate:
            custom['dateExtent'] = self.make_date_extent()

        with open(output_path, 'w') as f:
            json.dump(custom, f, indent=2, ensure_ascii=False)

    def lazy_ref_map(self):
        pass

    def enrich(self, gd):
        for gn in gd['nodes']:
            node_idx = gn['node']
            variant = self.node2variant[node_idx]

            if type(variant) is str:
                gn['name'] = self.person_map.get(variant, variant)

                town_name = self.hamlet2town.get(variant)
                if town_name:
                    gn['ext_url'] = "%s/%s" % (town_url_head, town_name)
            else:
                gn['name'] = self.party_map[variant]

            gn['color'] = self.introduce_color(variant)

        # D3 sankey calls weight "value"...
        for lnk in gd['links']:
            weight = lnk.pop('weight')
            lnk['value'] = weight

        if self.mindate and self.maxdate:
            gd['dateExtent'] = self.make_date_extent()

    def make_matrix(self):
        matrix = []
        n = len(self.pair2node)
        for i in range(n):
            matrix.append([ 0 ] * n)

        for edge, weight in self.ref_map.items():
            row = matrix[edge[0]]
            row[edge[1]] = weight

        return matrix

    def make_desc(self):
        desc = []
        n = len(self.pair2node)
        for i in range(n):
            variant = self.node2variant[i]
            name = self.get_presentation_name(variant)
            color = self.introduce_color(variant)
            desc.append({'name': name, 'color': color})

        return desc

    def make_matrix_desc(self):
        return {}

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]

    def introduce_node(self, variant, from_flag):
        node_idx = self.pair2node.get((variant, from_flag))
        if node_idx is None:
            node_idx = len(self.pair2node) # 0-based
            self.pair2node[(variant, from_flag)] = node_idx
            self.node2variant[node_idx] = variant

        return node_idx
