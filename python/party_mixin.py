from opt_util import get_quoted_list_option
from personage import normalize_name

def by_reverse_value(p):
    return (-1 * p[1], p[0])


class PartyMixin: # self.cur must be provided by another inherited class
    def __init__(self):
        self.party_map = {} # int party id -> str (short) party name
        self.person_map = {} # str hamlet name -> str presentation name
        self.hamlet2party = {} # str -> int
        self.party2color = {} # int party id / 0 -> str color (6 chars, w/o #)
        self.init_mapping()
        self.next_shade = 'A'

    def init_mapping(self):
        self.cur.execute("""select hamlet_name, presentation_name, steno_record.party_id, party_name, color
from steno_record
left join steno_party on steno_party.id=steno_record.party_id
left join steno_party_name on steno_party.id=steno_party_name.party_id
order by hamlet_name""")
        rows = self.cur.fetchall()
        for hamlet_name, present_name, party_id, party_name, color in rows:
            self.person_map[hamlet_name] = present_name

            if party_id:
                self.hamlet2party[hamlet_name] = party_id

                old_name = self.party_map.get(party_id)
                if (old_name is None) or (len(old_name) > len(party_name)):
                    self.party_map[party_id] = party_name

                if color:
                    self.party2color[party_id] = color

    def restrict_persons(self):
        names = get_quoted_list_option("selected_individuals", None)
        if not names:
            raise Exception("must specify selected_individuals option")

        selected_names = set() # of hamlet name
        for name in names:
            selected_names.add(self.ensure_name(name))

        person_map = {}
        for hamlet_name, present_name in self.person_map.items():
            if hamlet_name in selected_names:
                person_map[hamlet_name] = present_name

        self.person_map = person_map

    def convert_color(self, party_id):
        color = self.party2color.get(party_id)
        if not color:
            color = self.next_shade * 6
            self.next_shade = chr(ord(self.next_shade) + 1)
            if self.next_shade == 'D': # independents have that
                self.next_shade = 'A'

            self.party2color[party_id] = color

        return '#' + color

    def ensure_name(self, raw_name):
        name = normalize_name(raw_name)
        mask = '%' + name + '%'
        self.cur.execute("""select hamlet_name
from steno_record
where presentation_name ilike %s
and card_url_id is not null""", (mask,))
        rows = self.cur.fetchall()
        l = len(rows)
        if l != 1:
            raise Exception("%s matched %d records" % (raw_name, l))

        row = rows[0]
        return row[0]
