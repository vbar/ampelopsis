#!/usr/bin/python3

import json
import sys
from common import get_option, make_connection
from show_case import ShowCase
from token_util import tokenize

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.length2count = {}
        self.total = 0

    def load_item(self, att):
        self.extend_date(att)
        self.total += 1

        txt = att.get('DocumentPlainText')
        if not txt:
            length = 0
        else:
            lst = tokenize(txt)
            length = len(lst)

        cnt = self.length2count.get(length, 0)
        self.length2count[length] = 1 + cnt

    def dump(self):
        custom = {
            'total': self.total
        }

        l2c = []
        for k, v in sorted(self.length2count.items()):
            l2c.append([k, v])

        custom['length2count'] = l2c
        custom['dateExtent'] = self.make_date_extent()

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
