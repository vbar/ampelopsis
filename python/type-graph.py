#!/usr/bin/python3

import json
import sys
from common import get_option, make_connection
from show_cabinet import ShowCabinet

class Processor(ShowCabinet):
    def __init__(self, cur):
        ShowCabinet.__init__(self, cur)
        self.type2count = {}

    def load_item(self, att):
        self.extend_date(att)

        tp = att['typ']
        cnt = self.type2count.get(tp, 0)
        self.type2count[tp] = 1 + cnt

    def dump(self):
        custom = {
            'type2count': self.type2count,
            'dateExtent': self.make_date_extent()
        }

        json.dump(custom, sys.stdout, indent=2, ensure_ascii=False)


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
