#!/usr/bin/python3

# requires database created with config option jump_links = 2

import re
import sys
from common import make_connection
from json_lookup import JsonLookup
from tree_check import normalize_value, TreeCheck

TITLE_BEFORE = 1

TITLE_AFTER = 2


def make_opt_regex(raw):
    if raw is None:
        return None

    if not raw:
        raise Exception("empty title not supported")

    return re.compile("\\b%s\\b" % re.escape(raw.lower()))


class TitleFilter(JsonLookup):
    def __init__(self, cur, title_before, title_after):
        JsonLookup.__init__(self, cur)

        self.filter_check = TreeCheck()
        if title_before:
            self.filter_check.add('titleBefore', title_before, 'TITLE_BEFORE')

        if title_after:
            self.filter_check.add('titleAfter', title_after, 'TITLE_AFTER')

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

        found_set = self.filter_check.find(detail)
        if not len(found_set):
            return

        found = False
        generic_url = self.make_query_single_url(detail, set())
        guha = self.has_answer(generic_url)
        position_set = self.make_position_set(detail)
        l = len(position_set)
        if l:
            specific_urls = self.make_query_urls(detail, position_set)
            suha = any(self.has_answer(su) for su in specific_urls)
            if (not suha) and guha:
                found = True
        elif guha:
            found = True

        if found:
            print(url)

    def has_answer(self, url):
        doc = self.get_document(url)
        if not doc:
            return False

        bindings = doc['results']['bindings']
        return len(bindings)


def main():
    mode = 0
    before = None
    after = None
    for a in sys.argv[1:]:
        if a in ( '-tb', '--title-before' ):
            if mode:
                raise Exception("missing argument")

            if before is not None:
                raise Exception("title-before cannot repeat")

            mode = TITLE_BEFORE
        elif a in ( '-ta', '--title-after' ):
            if mode:
                raise Exception("missing argument")

            if after is not None:
                raise Exception("title-after cannot repeat")

            mode = TITLE_AFTER
        else:
            if mode == TITLE_BEFORE:
                before = a
                mode = 0
            elif mode == TITLE_AFTER:
                after = a
                mode = 0
            else:
                raise Exception("unknown argument " + a)

    if (before is None) and (after is None):
        raise Exception("must specify title-before and/or title-after")

    with make_connection() as conn:
        with conn.cursor() as cur:
            flt = TitleFilter(cur, normalize_value(before), normalize_value(after))
            flt.run()

if __name__ == "__main__":
    main()
