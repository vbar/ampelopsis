#!/usr/bin/python3

# requires database filled by running condensate.py

import csv
import datetime
from dateutil.parser import parse
import json
import sys
from common import get_option, make_connection
from show_case import ShowCase

DATE_NAME = 'date'
OTHER_NAME = 'nezařazení'

class Timeline(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.party2color = {} # long party name => str color; initialized from this ctor
        self.person2party = {} # hamlet name => long party name, or OTHER_NAME; filled lazily
        self.party2timeline = {} # long party name, or OTHER_NAME => list of datetime; output
        self.now_sorted = True # empty is sorted
        self.init_color()

    def init_color(self):
        self.cur.execute("""select long_name, color
from vn_party
where color is not null
order by long_name""")
        rows = self.cur.fetchall()
        for row in rows:
            if row[0] in (DATE_NAME, OTHER_NAME):
                raise Exception("reserved name %s already in database" % row[0])

            self.party2color[row[0]] = row[1]

    def is_empty(self):
        return not len(self.party2timeline)

    def get_min_date(self):
        self.lazy_sort()
        dt = None
        for party, timeline in self.party2timeline.items():
            if (dt is None) or (timeline[0] < dt):
                dt = timeline[0]

        return dt

    def get_max_date(self):
        self.lazy_sort()
        dt = None
        for party, timeline in self.party2timeline.items():
            if (dt is None) or (timeline[-1] > dt):
                dt = timeline[-1]

        return dt

    def get_timelines(self):
        self.lazy_sort()
        return self.party2timeline

    def get_colors(self):
        party2color = {}
        for party, color in self.party2color.items():
            party2color[party] = '#' + color

        shade = 'A'
        for party in self.party2timeline:
            if party not in party2color:
                color = shade * 6
                party2color[party] = '#' + color
                shade = chr(ord(shade) + 1)
                if shade == 'D': # independents have that
                    shade = 'A'

        return party2color

    def get_party(self, hamlet_name):
        party_name = self.person2party.get(hamlet_name)
        if party_name:
            return party_name

        self.cur.execute("""select long_name
from vn_record
join vn_party on party_id=vn_party.id
where hamlet_name=%s""", (hamlet_name,))
        row = self.cur.fetchone()
        party_name = row[0] if row else OTHER_NAME
        self.person2party[hamlet_name] = party_name
        return party_name

    def restrict_parties(self, party_limit):
        if len(self.party2timeline) <= party_limit:
            return

        counts = []
        for party, timeline in self.party2timeline.items():
            counts.append(len(timeline))

        # FIXME: use heap
        counts.sort(reverse=True)
        threshold = counts[party_limit - 1]

        party2timeline = {}
        party2color = {}
        for party, timeline in self.party2timeline.items():
            if len(self.party2timeline[party]) >= threshold:
                party2timeline[party] = timeline
                color = self.party2color.get(party)
                if color:
                    party2color[party] = color

        self.party2timeline = party2timeline
        self.party2color = party2color

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            pdt = parse(et.get('datum'))
            dt = pdt.replace(microsecond=0, second=0, minute=0)
            hamlet_name = et.get('osobaid')
            if hamlet_name:
                party_name = self.get_party(hamlet_name)
                timeline = self.party2timeline.get(party_name)
                if not timeline:
                    timeline = [ dt ]
                    self.party2timeline[party_name] = timeline
                else:
                    timeline.append(dt)
                    self.now_sorted = False

    def lazy_sort(self):
        if self.now_sorted:
            return

        party2timeline = {}
        for party, timeline in self.party2timeline.items():
            party2timeline[party] = sorted(timeline)

        self.party2timeline = party2timeline
        self.now_sorted = True


def get_model(builder):
    xseries = []
    value_series = {}
    timeline_map = builder.get_timelines()
    delta = datetime.timedelta(hours=1)
    dt = builder.get_min_date()
    maxdt = builder.get_max_date()
    idx_map = {}
    while dt <= maxdt:
        xseries.append(dt)
        for party, timeline in timeline_map.items():
            l = len(timeline)
            idx = idx_map.get(party, 0)
            freq = 0
            while (idx < l) and (dt == timeline[idx]):
                freq += 1
                idx += 1

            vseries = value_series.get(party)
            if not vseries:
                vseries = [ freq ]
                value_series[party] = vseries
            else:
                vseries.append(freq)

            idx_map[party] = idx

        dt += delta

    return (xseries, value_series)


def dump_meta(color_map):
    target = get_option("datetimes_meta_data", "datetimes.json")
    with open(target, 'w') as f:
        json.dump(color_map, f, indent=2, ensure_ascii=False)


def dump_content(xseries, value_series):
    target = get_option("datetimes_data", "datetimes.csv")
    tail_keys = sorted(value_series.keys())
    with open(target, 'w') as f:
        writer = csv.writer(f, delimiter=",")
        headings = [ DATE_NAME ]
        headings.extend(tail_keys)
        writer.writerow(headings)

        for i in range(len(xseries)):
            row = [ xseries[i] ]
            for k in tail_keys:
                vseries = value_series[k]
                row.append(vseries[i])

            writer.writerow(row)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            builder = Timeline(cur)
            try:
                builder.run()
                if builder.is_empty():
                    return

                pl = int(get_option("party_limit", "0"))
                if pl > 0:
                    builder.restrict_parties(pl)

                xseries, value_series = get_model(builder)
                dump_meta(builder.get_colors())
                dump_content(xseries, value_series)

            finally:
                builder.close()


if __name__ == "__main__":
    main()
