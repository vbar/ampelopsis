#!/usr/bin/python3

import re
import sys
from common import make_connection
from show_case import ShowCase

class Scanner(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.tag_rx = re.compile('#([-\\w]+)')
        self.tag_map = {}

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            txt = et.get('text')
            for m in self.tag_rx.finditer(txt):
                tag = m.group(1)
                cnt = self.tag_map.get(tag, 0)
                self.tag_map[tag] = cnt + 1

    def dump(self):
        for tag, cnt in sorted(self.tag_map.items(), key=lambda p: (-1 * p[1], p[0])):
            print(tag, "\t", cnt)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur)
            scanner.run()
            scanner.dump()


if __name__ == "__main__":
    main()
