#!/usr/bin/python3

# requires database filled by running condensate.py

from common import get_option, make_connection
from pinhole_base import PinholeBase
from token_util import tokenize
from datetime import datetime
import json
import sys

class Processor(PinholeBase):
    def __init__(self, cur):
        PinholeBase.__init__(self, cur, False, '*')
        self.variant2count = {}
        self.date2line = {} # datetime => list of record
        self.now = datetime.now()
        self.max_age_sec = 5 * 365 * 24 * 3600

    def load_item(self, rec):
        ext_url = rec['url']

        hamlet_name = rec['OsobaId']
        variant = self.get_variant(hamlet_name)
        if not variant:
            return

        oldmindate = self.mindate
        dt = self.extend_date(rec)
        if not dt:
            return

        td = self.now - dt.replace(tzinfo=None)
        if td.total_seconds() > self.max_age_sec:
            # record ignored
            self.mindate = oldmindate
            return

        cnt = self.variant2count.get(variant, 0)
        self.variant2count[variant] = 1 + cnt

        line = self.date2line.get(dt)
        if line is None:
            self.date2line[dt] = [ rec ]
        else:
            line.append(rec)

    def dump(self):
        keys = [ p[0] for p in sorted(self.variant2count.items(), key=lambda q: (-1 * q[1], str(q[0]))) ]
        indirect = {}
        names = []
        colors = []
        for idx, variant in enumerate(keys):
            present_name = self.get_presentation_name(variant)
            var_color = self.introduce_color(variant)
            indirect[variant] = idx
            names.append(present_name)
            colors.append(var_color)

        descriptions = []
        daylines = []
        for dt, line in sorted(self.date2line.items(), key=lambda p: p[0]):
            timeline = []
            total = 0
            for rec in sorted(line, key=lambda r: int(r['poradi'])):
                hamlet_name = rec['OsobaId']
                variant = self.get_variant(hamlet_name)
                url = self.get_circuit_url(rec)
                lst = tokenize(rec['text'])
                length = len(lst)
                total += length

                item = [ indirect[variant], length, url ]
                timeline.append(item)

            desc = [ dt.isoformat(), total ]
            descriptions.append(desc)
            daylines.append(timeline)

        custom = {
            'names': names,
            'colors': colors,
            'dayDesc': descriptions,
            'dayLines': daylines,
            'dateExtent': self.make_date_extent()
        }

        json.dump(custom, sys.stdout, indent=2, ensure_ascii=False)

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]


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
