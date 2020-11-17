#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import sys
from common import make_connection
from party_mixin import PartyMixin
from show_case import ShowCase
from timecycle_mixin import TimecycleMixin

class Processor(ShowCase, PartyMixin, TimecycleMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        TimecycleMixin.__init__(self)

    def dump(self):
        results = sorted(self.hamlet2per.items(), key=lambda p: (-1 * self.get_variance(p[1]), p[0]))
        for hamlet_name, perfreq in results[:10]:
            present_name = self.person_map[hamlet_name]
            print("%s\t%.3f" % (present_name, self.get_variance(perfreq)), file=sys.stderr)

    def load_item(self, et):
        hamlet_name = et['osobaid']
        perfreq = self.hamlet2per.get(hamlet_name)
        if perfreq is None:
            perfreq = [0] * self.period_size
            self.hamlet2per[hamlet_name] = perfreq

        dt = self.extend_date(et)
        perfreq[self.part_extractor(dt)] += 1

    def get_variance(self, perfreq):
        n = len(perfreq)
        avg = sum(perfreq) / n
        return sum(( (x - avg) * (x - avg) for x in perfreq )) / n


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
