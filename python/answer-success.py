#!/usr/bin/python3

import sys
from common import make_connection
from json_lookup import JsonLookup

GENERIC_INDEX = 0

SPECIFIC_INDEX = 1

class Scanner(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)
        self.doc_count = 0
        self.query_count = 0
        self.seen = set() # of url_id
        self.skip_count = 0
        self.total = [0, 0]
        self.success = [0, 0]

    def run(self):
        self.cur.execute("""select url
from field
left join download_error on id=url_id
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
and checkd is not null
and url_id is null
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.scan(row[0])

        print("%d duplicate URLs skipped" % self.skip_count, file=sys.stderr)

    def dump(self):
        word = ('generic', 'specific')
        for idx in range(len(word)):
            perc = 100 * self.success[idx] / self.total[idx]
            print("%d/%d (%.2f%%) %s answers found" % (self.success[idx], self.total[idx], perc, word[idx]))

    def scan(self, url):
        self.doc_count += 1
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        name_rx = self.make_name_rx(detail)

        generic_url = self.make_query_single_url(detail, set())
        self.process(generic_url, name_rx, GENERIC_INDEX)

        position_set = self.make_position_set(detail)
        l = len(position_set)
        if l:
            specific_urls = self.make_query_urls(detail, position_set)
            for specific_url in specific_urls:
                self.process(specific_url, name_rx, SPECIFIC_INDEX)

    def process(self, url, name_rx, index):
        self.query_count += 1
        if not (self.query_count % 1000):
            print("%d queries from %d documents" %
                  (self.query_count, self.doc_count), file=sys.stderr)

        url_id = self.get_url_id(url)
        if not url_id:
            return

        if url_id in self.seen:
            self.skip_count += 1
            return
        else:
            self.seen.add(url_id)

        try:
            doc = self.get_document(url)
        except:
            doc = None

        if not doc:
            return

        self.total[index] += 1
        if self.has_name(doc['results']['bindings'], name_rx):
            self.success[index] += 1

    # real lookup is stricter: this method doesn't require date nor wikidata ID
    def has_name(self, bindings, name_rx):
        for it in bindings:
            if name_rx.search(it['l']['value']):
                return True

        return False

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur)
            scanner.run()
            scanner.dump()


if __name__ == "__main__":
    main()
