#!/usr/bin/python3

# requires database created with config option jump_links = 2

import json
import re
import sys
from common import make_connection
from json_lookup import JsonLookup
from rulebook_util import get_org_name
from urlize import extract_query

UNDERSPECIFIED = 1

OVERSPECIFIED = 2

def print_query(qurl):
    q = extract_query(qurl)
    if (q):
        print(q)

    print("")

class DiFilter(JsonLookup):
    def __init__(self, cur, mode, verbose, req_org_name=None, req_pos_name=None):
        JsonLookup.__init__(self, cur)
        self.mode = mode
        self.verbose = verbose
        self.req_org_name = req_org_name
        self.req_pos_name = req_pos_name
        self.black = None
        self.white = None
        self.entity_rx = re.compile('/(Q[0-9]+)$')

    def set_blacklist(self, blacklist):
        self.black = set(blacklist)
        self.white = None

    def set_whitelist(self, whitelist):
        self.black = None
        self.white = set(whitelist)

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


        if not self.has_req_name(detail):
            return

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

    def has_req_name(self, detail):
        if not self.req_org_name and not self.req_pos_name:
            # no filtering
            return True

        lst = detail['workingPositions']
        for it in lst:
            if self.req_org_name:
                nm = get_org_name(it)
                if nm == self.req_org_name:
                    return True

            if self.req_pos_name:
                wp = it['workingPosition']
                nm = wp['name']
                if nm == self.req_pos_name:
                    return True

        return False

    # doesn't respect filtering in JsonLookup.get_entities - should it?
    def has_answer(self, url):
        assert (self.black is None) or (self.white is None)

        doc = self.get_document(url)
        if not doc:
            return False

        bindings = doc['results']['bindings']
        if (self.black is None) and (self.white is None):
            return len(bindings)

        for it in bindings:
            m = self.entity_rx.search(it['p']['value'])
            if m:
                pos = m.group(1)
                if self.white is None:
                    if not(pos in self.black):
                        return True
                else:
                    if pos in self.white:
                        return True

        return False


def main():
    argv = sys.argv[:]

    entity_rx = re.compile('^Q[1-9][0-9]*$')
    tail_list = []
    black_flag = False
    while len(argv):
        if entity_rx.match(argv[-1]):
            tail_list.insert(0, argv.pop())
        else:
            break

    if len(argv):
        a = argv[-1]
        if a in ('-b', '--black'):
            argv.pop()
            black_flag = True
        elif a in ('-w', '--white'):
            argv.pop()
            # black_flag remains false

    args = []
    verbose = False
    pos_name_filter = False
    org_name_filter = False
    for a in argv[1:]:
        if pos_name_filter is None:
            pos_name_filter = a
        elif org_name_filter is None:
            org_name_filter = a
        elif a in ( '-pn', '--pos-name' ):
            pos_name_filter = None
        elif a in ( '-on', '--org-name' ):
            org_name_filter = None
        elif a in ( '-v', '--verbose' ):
            verbose = True
        else:
            args.append(a)

    for a in ( pos_name_filter, org_name_filter ):
        if (a is None) or (a == ""):
            raise Exception("missing required option value")

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
            difilter = DiFilter(cur, mode, verbose, req_org_name=org_name_filter, req_pos_name=pos_name_filter)
            if len(tail_list):
                if black_flag:
                    difilter.set_blacklist(tail_list)
                else:
                    difilter.set_whitelist(tail_list)

            difilter.run()

if __name__ == "__main__":
    main()
