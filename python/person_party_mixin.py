from party_mixin import PartyMixin

class PersonPartyMixin(PartyMixin):
    def __init__(self, deconstructed):
        PartyMixin.__init__(self)

        if deconstructed == '*':
            self.deconstructed = None
        else:
            self.deconstructed = set() # of int party id
            self.init_deconstructed(deconstructed)

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

    def introduce_color(self, variant):
        if type(variant) is str:
            party_id = self.hamlet2party.get(variant, 0)
        else:
            party_id = variant

        return self.convert_color(party_id)
