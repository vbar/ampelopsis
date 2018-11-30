#!/usr/bin/python3

import sys
from common import make_connection
from json_lookup import JsonLookup

class Statistic:
    def __init__(self, msg, error_phrase = "errors"):
        self.msg = msg
        self.error_phrase = error_phrase
        self.total = 0
        self.error = 0

    def inc(self):
        self.total += 1

    def inc_error(self):
        self.error += 1

    def dump(self):
        error = " + %d %s" % (self.error, self.error_phrase) if self.error else ""
        print("%s: %d%s" % (self.msg, self.total, error))

class Builder(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)
        self.total = Statistic("total")
        self.interpreted = Statistic("position checked")
        self.found = Statistic("found single", error_phrase = "multiple")

    def run(self):
        self.cur.execute("""select url, id
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.add(*row)

        self.total.dump()
        self.interpreted.dump()
        self.found.dump()

    def add(self, url, url_id):
        print("checking " + url + "...", file=sys.stderr)

        try:
            detail = self.get_document(url)
        except:
            detail = None

        if not detail:
            self.total.inc_error()
            return

        self.total.inc()

        position_set = self.make_position_set(detail)
        if not len(position_set):
            return

        self.interpreted.inc() # error is never incremented

        try:
            persons = self.get_entities(detail)
        except:
            persons = []

        l = len(persons)
        if l == 1:
            self.found.inc()
        elif l > 1:
            self.found.inc_error()

def main():

    with make_connection() as conn:
        with conn.cursor() as cur:
            stat = Builder(cur)
            stat.run()

if __name__ == "__main__":
    main()
