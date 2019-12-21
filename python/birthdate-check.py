#!/usr/bin/python3

import re
import sys
from common import make_connection
from json_lookup import JsonLookup


class Checker(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)
        self.doc_count = 0
        self.query_count = 0
        self.prec2wids = {} # int (wikibase:timePrecision) -> set of str (entity)

    def fill(self):
        self.cur.execute("""select url
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.check(row[0])

    def dump(self):
        precs = sorted(self.prec2wids.keys())
        for p in precs:
            print("%d: %d IDs" % (p, len(self.prec2wids[p])))

    def check(self, url):
        self.doc_count += 1
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        generic_url = self.make_query_single_url(detail, set())
        guha = self.check_answer(generic_url)
        position_set = self.make_position_set(detail)
        l = len(position_set)
        if l:
            specific_urls = self.make_query_urls(detail, position_set)
            for specific_url in specific_urls:
                self.check_answer(specific_url)

    def check_answer(self, url):
        self.query_count += 1
        if not (self.query_count % 1000):
            print("%d queries from %d documents" %
                  (self.query_count, self.doc_count), file=sys.stderr)

        doc = self.get_document(url)
        if not doc:
            return False

        bindings = doc['results']['bindings']
        for binding in bindings:
            w = binding['w']['value']
            p = int(binding['n']['value'])
            wids = self.prec2wids.get(p)
            if wids is None:
                wids = set()
                self.prec2wids[p] = wids

            wids.add(w)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            chkr = Checker(cur)
            chkr.fill()
            chkr.dump()

if __name__ == "__main__":
    main()
