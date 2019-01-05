#!/usr/bin/python3

import re
import sys
from common import make_connection
from json_lookup import JsonLookup

CHATTELS = 1

class Selector(JsonLookup):
    def __init__(self, cur, sel):
        JsonLookup.__init__(self, cur)
        self.sel = sel

    def run(self):
        self.cur.execute("""select url
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.test(row[0])

    def test(self, url):
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        found = False
        for statement in detail['statements']:
            for k in self.sel:
                if (k in statement) and self.has_non_empty(statement[k]):
                    found = True

            if found:
                print(url)
                return

    # input data often contain non-empty array of empty objects...
    def has_non_empty(self, arr):
        for item in arr:
            if (type(item) is not dict) or len(item.keys()):
                return True

        return False

def main():
    sel = set()
    arg_rx = re.compile("^--(.+)$")
    for a in sys.argv[1:]:
        m = arg_rx.match(a)
        if m:
            b = m.group(1)
        else:
            b = a

        # not in data: incomesBefore, legalBusinessAssociatesBefore, liabilitiesBefore
        if b in ( 'chattels', 'chattelsBefore', 'commercialPapers', 'commercialPapersBefore', 'corporationShares', 'corporationSharesBefore', 'incomes', 'legalBusinessAssociates', 'liabilities', 'media', 'naturalPersonBusinesses', 'organizationMember', 'otherContracts', 'realties', 'realtiesBefore' ):
            sel.add(b)
        else:
            raise Exception("unknown argument " + a)

    if not len(sel):
        sel.add('chattels')

    with make_connection() as conn:
        with conn.cursor() as cur:
            selector = Selector(cur, sel)
            selector.run()

if __name__ == "__main__":
    main()
