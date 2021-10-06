#!/usr/bin/python3

# requires database filled by running condensate.py

import csv
import json
from common import get_option, make_connection
from known_names import KnownNames
from party_mixin import by_reverse_value, PartyMixin
from show_case import ShowCase
from url_heads import town_url_head


class Volume(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.person2count = {}
        self.party2total = {}

    def dump(self):
        custom = {
            'sparse': self.make_sparse(),
            'rowDesc': self.make_person_list(),
            'colDesc': self.make_party_list(),
            'persons': self.make_payload(),
            'colors': self.make_meta(),
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        self.extend_date(et)
        hamlet_name = et['osobaid']
        cnt = self.person2count.get(hamlet_name, 0)
        self.person2count[hamlet_name] = cnt + 1

        name = self.person_map.get(hamlet_name)
        if name:
            party_id = self.hamlet2party.get(hamlet_name, 0)
            ttl = self.party2total.get(party_id, 0)
            self.party2total[party_id] = ttl + 1

    def make_party_list(self):
        parties = []
        for party_id, ttl in sorted(self.party2total.items(), key=by_reverse_value):
            party_name = self.party_map.get(party_id, KnownNames.OTHER_NAME)
            parties.append(party_name)

        return parties

    def make_person_list( self):
        persons = []

        for hamlet_name, cnt in sorted(self.person2count.items(), key=by_reverse_value):
            name = self.person_map.get(hamlet_name)
            if name:
                persons.append(name)

        return persons

    def make_sparse(self):
        matrix = []
        for hamlet_name, cnt in sorted(self.person2count.items(), key=by_reverse_value):
            name = self.person_map.get(hamlet_name)
            if name:
                cur_party_id = self.hamlet2party.get(hamlet_name, 0)
                pair = None
                idx = 0
                for party_id, ttl in sorted(self.party2total.items(), key=by_reverse_value):
                    if party_id == cur_party_id:
                        pair = [idx, cnt]
                        break

                    idx += 1

                assert pair
                matrix.append(pair)

        return matrix

    def make_payload(self):
        name2payload = {}
        for hamlet_name in self.person2count:
            name = self.person_map.get(hamlet_name)
            if name:
                party_id = self.hamlet2party.get(hamlet_name, 0)
                party_name = self.party_map.get(party_id, KnownNames.OTHER_NAME)
                payload = { 'party': party_name }
                town_name = self.hamlet2town.get(hamlet_name)
                if town_name:
                    payload['ext_url'] = "%s/%s" % (town_url_head, town_name)

                name2payload[name] = payload

        return name2payload

    def make_meta(self):
        party2color = {}
        for hamlet_name in self.person2count:
            name = self.person_map.get(hamlet_name)
            if name:
                party_id = self.hamlet2party.get(hamlet_name, 0)
                party_name = self.party_map.get(party_id, KnownNames.OTHER_NAME)
                if party_name not in party2color:
                    color = self.convert_color(party_id)
                    party2color[party_name] = color

        return party2color

    def make_date_extent(self):
        # old D3 in frontend doesn't parse ISO format...
        return [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]

def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            vol = Volume(cur)
            try:
                vol.run()
                vol.dump()
            finally:
                vol.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
