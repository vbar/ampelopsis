#!/usr/bin/python3

# requires database created with config option jump_links = 2

import json
import os
import sys
from common import make_connection
from json_lookup import JsonLookup
from rulebook_util import get_org_name

class Scanner(JsonLookup):
    def __init__(self, cur, org_start):
        JsonLookup.__init__(self, cur)
        self.org_start = org_start
        self.names = set()

    def run(self):
        self.cur.execute("""select url
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.add(row[0])

        self.dump()

    def add(self, url):
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        lst = detail['workingPositions']
        for it in lst:
            nm = get_org_name(it)
            if nm.startswith(self.org_start):
                wp = it.get('workingPosition')
                if wp:
                    pnm = wp.get('name')
                    if pnm:
                        self.names.add(pnm)

    def dump(self):
        names = sorted(self.names)
        for pnm in names:
            print(pnm)

def main():
    l = len(sys.argv)
    if l != 2:
        raise Exception("usage: %s org_start" % sys.argv[0])

    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur, sys.argv[1])
            scanner.run()

if __name__ == "__main__":
    main()
