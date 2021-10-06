#!/usr/bin/python3

# requires database filled by running condensate.py

import csv
import datetime
from dateutil.parser import parse
import json
import sys
from common import get_option, make_connection
from known_names import KnownNames
from show_case import ShowCase
from timeline_mixin import TimelineMixin

class Timeline(ShowCase, TimelineMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        TimelineMixin.__init__(self, 'hours') # key is (shortest) party name, or OTHER_NAME
        self.party2color = {} # (any) party name => str color; initialized from this ctor
        self.person2party = {} # hamlet name => (shortest) party name, or OTHER_NAME; filled lazily
        self.init_color()

    def init_color(self):
        self.cur.execute("""select party_name, color
from vn_party
join vn_party_name on id=party_id
where color is not null
order by party_name""")
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] in (KnownNames.DATE_NAME, KnownNames.OTHER_NAME):
                raise Exception("reserved name %s already in database" % row[0])

            self.party2color[row[0]] = row[1]

    def get_meta(self):
        party2meta = {}
        shade = 'A'
        for party, timeline in self.key2timeline.items():
            color = self.party2color.get(party)
            if not color:
                color = shade * 6
                shade = chr(ord(shade) + 1)
                if shade == 'D': # independents have that
                    shade = 'A'

            party2meta[party] = { 'color': '#' + color, 'total': len(timeline) }

        return party2meta

    def get_party(self, hamlet_name):
        party_name = self.person2party.get(hamlet_name)
        if party_name:
            return party_name

        self.cur.execute("""select party_name
from vn_record
join vn_party on vn_record.party_id=vn_party.id
join vn_party_name on vn_party.id=vn_party_name.party_id
where hamlet_name=%s
order by length(party_name), party_name""", (hamlet_name,))
        row = self.cur.fetchone()
        party_name = row[0] if row else KnownNames.OTHER_NAME
        self.person2party[hamlet_name] = party_name
        return party_name

    def restrict_parties(self, party_limit):
        if len(self.key2timeline) <= party_limit:
            return

        counts = []
        for party, timeline in self.key2timeline.items():
            counts.append(len(timeline))

        # FIXME: use heap
        counts.sort(reverse=True)
        threshold = counts[party_limit - 1]

        key2timeline = {}
        party2color = {}
        for party, timeline in self.key2timeline.items():
            if len(self.key2timeline[party]) >= threshold:
                key2timeline[party] = timeline
                color = self.party2color.get(party)
                if color:
                    party2color[party] = color

        self.key2timeline = key2timeline
        self.party2color = party2color

    def load_item(self, et):
        hamlet_name = et['osobaid']
        dt = parse(et['datum'])
        party_name = self.get_party(hamlet_name)
        self.add_sample(party_name, dt)


def dump_meta(meta_map):
    target = get_option("datetimes_meta_data", "datetimes.json")
    with open(target, 'w') as f:
        json.dump(meta_map, f, indent=2, ensure_ascii=False)


def dump_content(xseries, value_series):
    target = get_option("datetimes_data", "datetimes.csv")
    tail_keys = sorted(value_series.keys())
    with open(target, 'w') as f:
        writer = csv.writer(f, delimiter=",")
        headings = [ KnownNames.DATE_NAME ]
        headings.extend(tail_keys)
        writer.writerow(headings)

        for i in range(len(xseries)):
            row = [ xseries[i] ]
            for k in tail_keys:
                vseries = value_series[k]
                row.append(vseries[i])

            writer.writerow(row)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            builder = Timeline(cur)
            try:
                builder.run()
                if builder.is_empty():
                    return

                pl = int(get_option("party_limit", "0"))
                if pl > 0:
                    builder.restrict_parties(pl)

                dump_meta(builder.get_meta())
                dump_content(builder.get_xseries(), builder.get_value_series())

            finally:
                builder.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
