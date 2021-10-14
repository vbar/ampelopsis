#!/usr/bin/python3

import json
import sys
from common import get_option, make_connection
from show_case import ShowCase
from token_util import tokenize

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.type2count = {}
        self.data = []

    def load_item(self, att):
        self.extend_date(att)

        tp = att['typ']
        cnt = self.type2count.get(tp, 0)
        self.type2count[tp] = 1 + cnt

        txt = att.get('DocumentPlainText')
        if not txt:
            length = 0
        else:
            lst = tokenize(txt)
            length = len(lst)

        if length:
            raw_url = att.get('DocumentUrl')
            url = self.get_circuit_url(raw_url) if raw_url else "???"
            item = [ url, tp, length ]
            self.data.append(item)

    def dump(self):
        keys = [ p[0] for p in sorted(self.type2count.items(), key=lambda q: (q[1], q[0])) ]
        indirect = {}
        types = []
        for idx, tp in enumerate(keys):
            indirect[tp] = idx
            types.append(tp)

        data = []
        for url, tp, length in self.data:
            item = [ url, indirect[tp], length ]
            data.append(item)

        custom = {
            'types': types,
            'data': data,
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
            processor.run()
            processor.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
