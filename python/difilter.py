#!/usr/bin/python3

# requires database created with config option jump_links = 2

import json
import sys
from urllib import parse
from common import make_connection
from json_lookup import JsonLookup

UNDERSPECIFIED = 1

OVERSPECIFIED = 2

def print_query(qurl):
    uo = parse.urlparse(qurl)
    params = parse.parse_qsl(uo.query)
    for p in params:
        if p[0] == 'query':
            print(p[1])

    print("")

class DiFilter(JsonLookup):
    def __init__(self, cur, mode, verbose):
        JsonLookup.__init__(self, cur)
        self.mode = mode
        self.verbose = verbose

    def run(self):
        self.cur.execute("""select url, id
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.test(*row)

    def test(self, url, url_id):
        detail = self.get_document(url)

        found = False
        specific_url = None
        generic_url = None
        position_set = self.make_position_set(detail)
        l = len(position_set)
        if (self.mode & OVERSPECIFIED) and l:
            specific_url = self.make_query_url(detail, position_set)
            generic_url = self.make_query_url(detail, set())
            if not self.has_answer(specific_url) and self.has_answer(generic_url):
                found = True

        if (self.mode & UNDERSPECIFIED) and not l:
            generic_url = self.make_query_url(detail, set())
            if self.has_answer(generic_url):
                found = True

        if found:
            print(url)
            if self.verbose:
                if specific_url:
                    self.print_qa(specific_url)

                self.print_qa(generic_url)
                print("")

    def print_qa(self, qurl):
        print_query(qurl)
        leaf = self.get_document(qurl)
        if leaf:
            json.dump(leaf, sys.stdout, ensure_ascii=False)
            print("")
            print("")

    def has_answer(self, url):
        doc = self.get_document(url)
        if not doc:
            return False

        bindings = doc['results']['bindings']
        return len(bindings)

def main():
    if len(sys.argv) > 4:
        raise Exception("too many arguments")

    args = []
    verbose = False
    for a in sys.argv[1:]:
        if (a == '-v') or (a == '--verbose'):
            verbose = True
        else:
            args.append(a)

    if len(args) > 2:
        raise Exception("too many arguments")

    modes = []
    for a in args:
        if (a == '-u') or (a == '--under'):
            modes.append(UNDERSPECIFIED)
        elif (a == '-o') or (a == '--over'):
            modes.append(OVERSPECIFIED)
        else:
            raise Exception("invalid argument " + a)

    l = len(modes)
    if l == 0:
        modes.append(OVERSPECIFIED)
    elif (l == 2) and (modes[0] == modes[1]):
        raise Exception("argument cannot repeat")

    with make_connection() as conn:
        with conn.cursor() as cur:
            mode = UNDERSPECIFIED | OVERSPECIFIED if len(modes) == 2 else modes[0]
            difilter = DiFilter(cur, mode, verbose)
            difilter.run()

if __name__ == "__main__":
    main()
