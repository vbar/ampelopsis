#!/usr/bin/python3

import json
import re
import sys
from common import get_option, make_connection
from show_room import ShowRoom

split_rx = re.compile(';\\s*')

class Processor(ShowRoom):
    def __init__(self, cur):
        ShowRoom.__init__(self, cur)
        self.catfreq = {} # str name -> str value -> int count

    def load_item(self, doc):
        self.extend_date(doc)

        for known in ('typMaterialu', 'predkladatel'):
            self.cond_increment(known, doc.get(known))

        composite = doc.get('klicovaSlova')
        if composite:
            lst = split_rx.split(composite)
            for kw in lst:
                self.cond_increment('klicovaSlova', kw)

    def dump(self):
        custom = {
            'categoryFrequency': self.catfreq,
            'dateExtent': self.make_date_extent()
        }

        json.dump(custom, sys.stdout, indent=2, ensure_ascii=False)

    def cond_increment(self, key, value):
        if not value:
            return

        name2count = self.catfreq.setdefault(key, {})
        cnt = name2count.get(value, 0)
        name2count[value] = 1 + cnt


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
