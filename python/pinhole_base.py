from show_case import ShowCase

class PinholeBase(ShowCase):
    def __init__(self, cur, distinguish, deconstructed):
        ShowCase.__init__(self, cur)
        self.distinguish = distinguish
        self.deconstructed = set() # of int party id
        self.init_deconstructed(deconstructed)
        self.party_map = {} # int party id -> str (short) party name
        self.person_map = {} # str hamlet name -> str presentation name
        self.hamlet2town = {} # str -> str
        self.town2hamlet = {} # str -> str
        self.hamlet2party = {} # str -> int
        self.party2color = {} # int party id / 0 -> str color (6 chars, w/o #)
        self.init_mapping()
        self.next_shade = 'A'
        self.pair2node = {} # (str hamlet name / int party id, bool from?) -> int node index
        self.node2variant = {} # int node index -> str hamlet name / int party id

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

    def init_mapping(self):
        self.cur.execute("""select hamlet_name, presentation_name, town_name, vn_record.party_id, party_name, color
from vn_record
join vn_identity_hamlet on record_id=vn_record.id
left join vn_party on vn_party.id=vn_record.party_id
left join vn_party_name on vn_party.id=vn_party_name.party_id
order by hamlet_name, town_name""")
        rows = self.cur.fetchall()
        for hamlet_name, present_name, town_name, party_id, party_name, color in rows:
            self.hamlet2town[hamlet_name] = town_name
            self.town2hamlet[town_name] = hamlet_name
            self.person_map[hamlet_name] = present_name
            if party_id:
                self.hamlet2party[hamlet_name] = party_id

                old_name = self.party_map.get(party_id)
                if (old_name is None) or (len(old_name) > len(party_name)):
                    self.party_map[party_id] = party_name

                if color:
                    self.party2color[party_id] = color

    def enrich(self, gd):
        for gn in gd['nodes']:
            node_idx = gn['node']
            variant = self.node2variant[node_idx]

            if type(variant) is str:
                gn['name'] = self.person_map[variant]

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

    def get_variant(self, hamlet_name):
        party_id = self.hamlet2party.get(hamlet_name)
        if party_id is None:
            return None

        return hamlet_name if party_id in self.deconstructed else party_id

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

        color = self.party2color.get(party_id)

        if not color:
            color = self.next_shade * 6
            self.next_shade = chr(ord(self.next_shade) + 1)
            if self.next_shade == 'D': # independents have that
                self.next_shade = 'A'

            self.party2color[party_id] = color

        return '#' + color
