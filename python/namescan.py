#!/usr/bin/python3

# requires database created with config option jump_links = 2

import json
import os
import sys
from common import make_connection
from json_lookup import JsonLookup
from rulebook_util import get_org_name

ORG = 1

WPN = 2

class Scanner(JsonLookup):
    def __init__(self, cur, feature, show_all):
        JsonLookup.__init__(self, cur)
        self.feature = feature
        self.show_all = show_all
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

        if not self.show_all:
            generic_url = self.make_query_url(detail, set())
            if not self.has_answer(generic_url):
                return

        lst = detail['workingPositions']
        for it in lst:
            nm = None
            if self.feature & ORG:
                nm = get_org_name(it)

            if self.feature & WPN:
                wp = it['workingPosition']
                nm = wp['name']

            if nm:
                self.names.add(nm)

    def has_answer(self, url):
        doc = None
        try:
            doc = self.get_document(url)
        except json.decoder.JSONDecodeError:
            pass

        if not doc:
            return False

        bindings = doc['results']['bindings']
        return len(bindings)

    def dump(self):
        names = sorted(self.names)
        for nm in names:
            print(nm)

def main():
    l = len(sys.argv)
    if l > 2:
        raise Exception("too many arguments")

    feature = 0
    show_all = False
    for a in sys.argv[1:]:
        if a in ( '-a', '--all' ):
            show_all = True
        elif a in ( '-pn', '--pos-name' ):
            feature |= WPN
        elif a in ( '-on', '--org-name' ):
            feature |= ORG
        else:
            raise Exception("invalid argument " + a)

    if feature == 0:
        feature = ORG
    elif (feature != WPN) and (feature != ORG):
        raise Exception("--org-name option is not compatible with --pos-name")

    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur, feature, show_all)
            scanner.run()

if __name__ == "__main__":
    main()
