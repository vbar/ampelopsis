#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import sys
from common import make_connection
from party_mixin import PartyMixin
from show_case import ShowCase

class Timeline(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.restrict_persons()
        self.hamlet2timeline = {}

    def dump(self):
        desc = []
        timelines = []
        for hamlet_name, src_tmln in sorted(self.hamlet2timeline.items(), key=lambda p: (-1 * len(p[1]), p[0])):
            present_name = self.person_map[hamlet_name]
            party_id = self.hamlet2party.get(hamlet_name, 0)
            desc.append({
                'name': present_name,
                'color': self.convert_color(party_id)
            })

            dst_tmln = []
            last = None
            for ext in sorted(src_tmln, key = lambda e: e['dt']):
                ed = ext['dt']
                url = ext['url']
                has_redirect = self.is_redirected(url)
                if ed == last:
                    p = dst_tmln[-1]
                    p[1].append(url)
                    p[2] = p[2] or has_redirect
                else:
                    dst_tmln.append([ed.isoformat(), [url], has_redirect])
                    last = ed

            timelines.append(dst_tmln)

        custom = {
            'dateExtent': self.make_date_extent(),
            "lineDesc": desc,
            "timelines": timelines
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        hamlet_name = et['osobaid']
        if hamlet_name not in self.person_map:
            return

        timeline = self.hamlet2timeline.get(hamlet_name)
        if not timeline:
            timeline = []
            self.hamlet2timeline[hamlet_name] = timeline

        dt = self.extend_date(et)
        et['dt'] = dt
        timeline.append(et)

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            timeline = Timeline(cur)
            try:
                timeline.run()
                timeline.dump()
            finally:
                timeline.close()


if __name__ == "__main__":
    main()
