#!/usr/bin/python3

import json
import sys
from common import make_connection
from json_lookup import JsonLookup
from jump_util import make_position_set, make_query_url

UNDERSPECIFIED = 1

OVERSPECIFIED = 2

class DiFilter(JsonLookup):
    def __init__(self, cur, mode):
        JsonLookup.__init__(self, cur)
        self.mode = mode

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

        found = None
        lst = detail['workingPositions']
        for it in lst:
            position_set = make_position_set(detail)
            l = len(position_set)
            if (self.mode & OVERSPECIFIED) and l:
                specific_url = make_query_url(detail, position_set)
                generic_url = make_query_url(detail, set())
                if not self.has_answer(specific_url) and self.has_answer(generic_url):
                    found = url

            if (self.mode & UNDERSPECIFIED) and not l:
                generic_url = make_query_url(detail, set())
                if self.has_answer(generic_url):
                    found = url

        if found:
            print(found)

    def has_answer(self, url):
        doc = self.get_document(url)
        if not doc:
            return False

        bindings = doc['results']['bindings']
        return len(bindings)

def main():
    if len(sys.argv) > 3:
        raise Exception("too many arguments")

    modes = []
    for a in sys.argv[1:]:
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
            difilter = DiFilter(cur, mode)
            difilter.run()

if __name__ == "__main__":
    main()
