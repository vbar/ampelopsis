import json
import networkx as nx
from party_mixin import PartyMixin
from show_case import ShowCase
from url_heads import town_url_head

class PinholeBase(ShowCase, PartyMixin):
    def __init__(self, cur, distinguish, deconstructed):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.distinguish = distinguish

        if deconstructed == '*':
            self.deconstructed = None
        else:
            self.deconstructed = set() # of int party id
            self.init_deconstructed(deconstructed)

        self.pair2node = {} # (str hamlet name / int party id, bool from?) -> int node index
        self.node2variant = {} # int node index -> str hamlet name / int party id
        self.ref_map = {} # (int, int) edge -> int weight

    def init_deconstructed(self, deco_list):
        if not deco_list:
            return

        deco_set = set(deco_list)
        self.cur.execute("""select party_id
from vn_party_name
where party_name in %s
order by party_id""", (tuple(deco_set),))
        rows = self.cur.fetchall()
        for row in rows:
            self.deconstructed.add(row[0])

    def dump_standard(self):
        self.lazy_ref_map()

        ebunch = [(edge[0], edge[1], weight) for edge, weight in self.ref_map.items()]
        graph = nx.DiGraph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def dump_custom(self, output_path):
        self.lazy_ref_map()

        custom = {
            'matrix': self.make_matrix(),
            'desc': self.make_desc()
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

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]

    def get_variant(self, hamlet_name):
        if self.deconstructed is None:
            return hamlet_name

        party_id = self.hamlet2party.get(hamlet_name)
        if party_id is None:
            return None

        return hamlet_name if party_id in self.deconstructed else party_id

    def get_presentation_name(self, variant):
        if type(variant) is str:
            return self.person_map[variant]
        else:
            return self.party_map[variant]

    def introduce_node(self, variant, from_flag):
        node_idx = self.pair2node.get((variant, from_flag))
        if node_idx is None:
            node_idx = len(self.pair2node) # 0-based
            self.pair2node[(variant, from_flag)] = node_idx
            self.node2variant[node_idx] = variant

        return node_idx

    def introduce_color(self, variant):
        if type(variant) is str:
            party_id = self.hamlet2party.get(variant, 0)
        else:
            party_id = variant

        return self.convert_color(party_id)
