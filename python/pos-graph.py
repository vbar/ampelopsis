#!/usr/bin/python3

# requires database filled by running condensate.py and downloaded
# data extended by running morphodita-stemmer.py

import json
from common import get_option, make_connection
from morphodita_tap import MorphoditaTap
from party_mixin import PartyMixin
from show_case import ShowCase
from url_heads import town_url_head

class Processor(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.restrict_persons()
        self.pos_head_length = int(get_option("pos_head_length", "1"))
        self.tap = MorphoditaTap(cur)
        self.characteristics = {} # str hamlet name -> str MorphoDiTa tag head -> int freq

    def dump(self):
        bulk = {}
        for hamlet_name, pos2freq in self.characteristics.items():
            bulk[hamlet_name] = sum(pos2freq.values())

        pos_set = set()
        freqs = []
        for hamlet_name, pos2freq in sorted(self.characteristics.items(), key=lambda p: (-1 * bulk[p[0]], p[0])):
            pos_set.update(pos2freq.keys())
            present_name = self.person_map[hamlet_name]
            party_id = self.hamlet2party.get(hamlet_name, 0)
            freq = {
                'name': present_name,
                'color': self.convert_color(party_id),
                'freq': pos2freq
            }

            town_name = self.hamlet2town.get(hamlet_name)
            if town_name:
                freq['ext_url'] = "%s/%s" % (town_url_head, town_name)

            freqs.append(freq)

        custom = {
            'pos': sorted(pos_set),
            'data': freqs,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        hamlet_name = et['osobaid']
        if hamlet_name not in self.person_map:
            return

        self.extend_date(et)
        pos2freq = self.characteristics.setdefault(hamlet_name, {})
        for tag in self.tap.get_tags(et['url']):
            if len(tag) >= self.pos_head_length:
                head = tag[:self.pos_head_length]
                c = pos2freq.get(head, 0)
                pos2freq[head] = c + 1

    def make_date_extent(self):
        # old D3 in frontend doesn't parse ISO format...
        return [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
