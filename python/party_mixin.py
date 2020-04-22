class PartyMixin: # self.cur must be provided by another inherited class
    def __init__(self):
        self.party_map = {} # int party id -> str (short) party name
        self.person_map = {} # str hamlet name -> str presentation name
        self.hamlet2town = {} # str -> str
        self.town2hamlet = {} # str -> str
        self.hamlet2party = {} # str -> int
        self.party2color = {} # int party id / 0 -> str color (6 chars, w/o #)
        self.init_mapping()
        self.next_shade = 'A'

    def init_mapping(self):
        self.cur.execute("""select hamlet_name, presentation_name, town_name, vn_record.party_id, party_name, color
from vn_record
left join vn_identity_hamlet on record_id=vn_record.id
left join vn_party on vn_party.id=vn_record.party_id
left join vn_party_name on vn_party.id=vn_party_name.party_id
order by hamlet_name, town_name""")
        rows = self.cur.fetchall()
        for hamlet_name, present_name, town_name, party_id, party_name, color in rows:
            if town_name:
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

    def convert_color(self, party_id):
        color = self.party2color.get(party_id)
        if not color:
            color = self.next_shade * 6
            self.next_shade = chr(ord(self.next_shade) + 1)
            if self.next_shade == 'D': # independents have that
                self.next_shade = 'A'

            self.party2color[party_id] = color

        return '#' + color
