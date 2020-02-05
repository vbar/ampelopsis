#!/usr/bin/python3

import re
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
        # duplicated from DownloadBase
        self.relative_rx = re.compile("^([0-9]{1,3})$")
        self.seen = set() # of url_id
        self.count429 = [0, 0]
        self.sum429 = [0, 0]

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

    def dump(self):
        print("%d+%d error responses requesting total of %d+%d wait seconds" %
              (self.count429[GENERIC_INDEX], self.count429[SPECIFIC_INDEX],
               self.sum429[GENERIC_INDEX], self.sum429[SPECIFIC_INDEX]))

    def scan(self, url):
        self.doc_count += 1
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        generic_url = self.make_query_single_url(detail, set())
        self.process(generic_url, GENERIC_INDEX)

        position_set = self.make_position_set(detail)
        l = len(position_set)
        if l:
            specific_urls = self.make_query_urls(detail, position_set)
            for specific_url in specific_urls:
                self.process(specific_url, SPECIFIC_INDEX)

    def process(self, url, index):
        self.query_count += 1
        if not (self.query_count % 1000):
            print("%d queries from %d documents" %
                  (self.query_count, self.doc_count), file=sys.stderr)

        self.cur.execute("""select id
from field
join download_error on id=url_id
where url=%s and error_code=429""", (url,))
        row = self.cur.fetchone()
        if not row:
            return

        url_id = row[0]
        if url_id in self.seen:
            return
        else:
            self.seen.add(url_id)

        volume_id = self.get_volume_id(url_id)
        t = self.get_retry_after(url_id, volume_id)
        if t is None:
            print(url + " returned 429 without Retry-After", file=sys.stderr)
        else:
            self.count429[index] += 1
            self.sum429[index] += t

    def get_retry_after(self, url_id, volume_id):
        raw_time = None
        f = self.open_headers(url_id, volume_id)
        if f is not None:
            try:
                for ln in f:
                    header_line = ln.decode('iso-8859-1')
                    line_list = header_line.split(':', 1)
                    if len(line_list) == 2:
                        name, value = line_list
                        name = name.strip()
                        name = name.lower()
                        if name == 'retry-after':
                            if raw_time is not None:
                                # probably won't happen...
                                raise Exception("Retry-After repeated")

                            m = self.relative_rx.match(value.strip())
                            if m:
                                raw_time = m.group(1)
            finally:
                f.close()

        if raw_time is None:
            return None

        return int(raw_time)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur)
            scanner.run()
            scanner.dump()


if __name__ == "__main__":
    main()
