#!/usr/bin/python3

# requires database filled by running condensate.py

import json
from common import get_option, make_connection
from party_mixin import PartyMixin
from show_case import ShowCase
from url_heads import town_url_head

granularity = {
    'weekday': ( 7, lambda dt: dt.weekday() ),
    'hour': ( 24, lambda dt: dt.hour ),
    'minute': ( 60, lambda dt: dt.minute ),
    'second': ( 60, lambda dt: dt.second )
}

class Processor(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.restrict_persons()
        self.cycle = get_option('time_cycle_period', 'weekday')
        spec = granularity.get(self.cycle)
        if not spec:
            raise Exception("Unknown time_cycle_period" + self.cycle)

        self.period_size = spec[0]
        self.part_extractor = spec[1]
        self.hamlet2per = {} # str hamlet name -> array of period_size ints

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
