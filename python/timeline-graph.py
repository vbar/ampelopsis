#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import sys
from common import get_mandatory_option, make_connection
from personage import normalize_name
from show_case import ShowCase

class Timeline(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.init_names(get_mandatory_option("selected_individual"))
        self.timeline = []

    def init_names(self, raw_name):
        name = normalize_name(raw_name)
        mask = '%' + name + '%'
        self.cur.execute("""select presentation_name, hamlet_name, party_id
from vn_record
where presentation_name ilike %s
and card_url_id is not null""", (mask,))
        rows = self.cur.fetchall()
        l = len(rows)
        if l != 1:
            raise Exception("%s matched % records" % (raw_name, l))

        row = rows[0]
        self.present_name = row[0]
        self.hamlet_name = row[1]
        self.party_id = row[2]

    def dump(self):
        timeline = []
        last = None
        for ext in sorted(self.timeline, key = lambda e: e['dt']):
            ed = ext['dt']
            url = ext['url']
            has_redirect = self.is_redirected(url)
            if ed == last:
                p = timeline[-1]
                p[1].append(url)
                p[2] = p[2] or has_redirect
            else:
                timeline.append([ed.isoformat(), [url], has_redirect])
                last = ed

        custom = {
            "lineDesc": {
                "name": self.present_name,
                "color": self.get_party_color()
            },
            "timeline": timeline
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        if et['osobaid'] != self.hamlet_name:
            return

        dt = self.extend_date(et)
        et['dt'] = dt
        self.timeline.append(et)

    def get_party_color(self):
        if self.party_id:
            self.cur.execute("""select color
from vn_party
where id=%s""", (self.party_id,))
            rows = self.cur.fetchall()
            for row in rows:
                return '#' + row[0]

        return '#AAAAAA'

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
