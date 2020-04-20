#!/usr/bin/python3

# requires download with funnel_links set to 1 and database filled by
# running condensate.py

import json
from common import make_connection
from known_names import KnownNames
from party_mixin import PartyMixin
from show_case import ShowCase


def by_reverse_value(p):
    return (-1 * p[1], p[0])


class Hierarchy(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.person2urls = {} # hamlet name -> set of status URL
        self.party2total = {}

    def dump(self):
        hier = {
            'children': self.make_parties(),
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(hier, indent=2))

    def load_item(self, et):
        self.extend_date(et)
        hamlet_name = et['osobaid']
        url = et['url']
        urls = self.person2urls.get(hamlet_name)
        if not urls:
            urls = set((url,))
        else:
            urls.add(url)

        self.person2urls[hamlet_name] = urls

        party_id = self.hamlet2party.get(hamlet_name, 0)
        ttl = self.party2total.get(party_id, 0)
        self.party2total[party_id] = ttl + 1

    def make_parties(self):
        parties = []
        for party_id, ttl in sorted(self.party2total.items(), key=by_reverse_value):
            party_name = self.party_map.get(party_id, KnownNames.OTHER_NAME)
            persons = self.make_persons(party_id)
            parties.append({
                'name': party_name,
                'color': self.convert_color(party_id),
                'children': persons,
                'colname': 'party'
            })

        return parties

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]

    def make_persons(self, party_id):
        persons = []
        for hamlet_name, urls in sorted(self.person2urls.items(), key=lambda p: (-1 * len(p[1]), p[0])):
            cur_id = self.hamlet2party.get(hamlet_name, 0)
            if cur_id == party_id:
                present_name = self.person_map.get(hamlet_name, "neznámý")
                statuses = self.make_statuses(hamlet_name)
                persons.append({
                    'name': present_name,
                    'children': statuses,
                    'colname': 'person'
                })

        return persons

    def make_statuses(self, hamlet_name):
        statuses = []
        urls = self.person2urls[hamlet_name]
        redirects = []
        for url in sorted(urls):
            if self.is_redirected(url):
                redirects.append(url)
            else:
                statuses.append({
                    'name': url,
                    'value': 1,
                    'colname': 'status'
                })

        for url in redirects:
            statuses.append({
                'name': url,
                'value': 0.5,
                'colname': 'status'
            })

        return statuses


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            hier = Hierarchy(cur)
            try:
                hier.run()
                hier.dump()
            finally:
                hier.close()


if __name__ == "__main__":
    main()
