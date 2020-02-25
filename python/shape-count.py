#!/usr/bin/python3

import re
import sys
from common import make_connection
from json_lookup import JsonLookup

number_rx = re.compile("^[0-9]+$")


def get_list_order(path):
    segments = path.split("/")
    s = 0
    for sg in segments:
        s = 10000 * s
        if number_rx.match(sg):
            s = s + int(sg)

    if s:
        l = len(segments)
        if l < 10:
            for i in range(10 - l):
                s = 10000 * s

    return s


class Scanner(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)
        self.doc_count = 0
        self.path2count = {}

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
        print("%d valid documents" % self.doc_count)

        for pair in sorted((p for p in self.path2count.items()), key=lambda q: (-1 * q[1], get_list_order(q[0]), q[0])):
            print(pair[1], 'x' , pair[0])

    def scan(self, url):
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        self.doc_count += 1
        if not (self.doc_count % 1000):
            print("%d..." % self.doc_count, file=sys.stderr)

        self.walk("", detail)

    def walk(self, path, node):
        if type(node) is dict:
            for k, v in node.items():
                self.walk("%s/%s" % (path, k), v)
        elif type(node) is list:
            idx = 0
            for it in node:
                self.walk("%s/%d" % (path, idx), it)
                idx += 1
        else:
            cnt = self.path2count.get(path, 0)
            self.path2count[path] = cnt + 1


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur)
            scanner.run()
            scanner.dump()


if __name__ == "__main__":
    main()
