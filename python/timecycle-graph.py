#!/usr/bin/python3

# requires database filled by running condensate.py

import json
from common import make_connection
from party_mixin import PartyMixin
from show_case import ShowCase
from timecycle_mixin import TimecycleMixin
from url_heads import town_url_head

class Processor(ShowCase, PartyMixin, TimecycleMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        TimecycleMixin.__init__(self)
        self.restrict_persons()

    def dump(self):
        freqs = []
        for hamlet_name, perfreq in sorted(self.hamlet2per.items(), key=lambda p: (-1 * sum(p[1]), p[0])):
            present_name = self.person_map[hamlet_name]
            party_id = self.hamlet2party.get(hamlet_name, 0)
            freq = {
                'name': present_name,
                'color': self.convert_color(party_id),
                'freq': perfreq
            }

            town_name = self.hamlet2town.get(hamlet_name)
            if town_name:
                freq['ext_url'] = "%s/%s" % (town_url_head, town_name)

            freqs.append(freq)

        custom = {
            'cycle': self.cycle,
            'data': freqs,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        hamlet_name = et['osobaid']
        if hamlet_name not in self.person_map:
            return

        perfreq = self.hamlet2per.get(hamlet_name)
        if perfreq is None:
            perfreq = [0] * self.period_size
            self.hamlet2per[hamlet_name] = perfreq

        dt = self.extend_date(et)
        perfreq[self.part_extractor(dt)] += 1

    def make_date_extent(self):
        # old D3 in frontend doesn't parse ISO format...
        return [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
