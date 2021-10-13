#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase
from token_util import tokenize

class Processor(PinholeBase):
    def __init__(self, cur, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        self.variant2count = {}
        self.data = []

    def load_item(self, et):
        ext_url = et['url']
        if self.is_redirected(ext_url):
            return

        hamlet_name = et['osobaid']
        variant = self.get_variant(hamlet_name)
        if not variant:
            return

        lst = tokenize(et['text'], True)
        length = len(lst)
        if not length: # empty text won't fit log scale
            return

        self.extend_date(et)

        cnt = self.variant2count.get(variant, 0)
        self.variant2count[variant] = 1 + cnt

        url = self.get_circuit_url(ext_url)

        item = ( url, variant, length )
        self.data.append(item)

    def dump(self):
        keys = [ p[0] for p in sorted(self.variant2count.items(), key=lambda p: (p[1], p[0])) ]
        indirect = {}
        names = []
        colors = []
        for idx, variant in enumerate(keys):
            indirect[variant] = idx
            names.append(self.get_presentation_name(variant))
            colors.append(self.introduce_color(variant))

        data = []
        for url, variant, length in self.data:
            item = [ url, indirect[variant], length ]
            data.append(item)

        custom = {
            'names': names,
            'colors': colors,
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
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
