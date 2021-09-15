#!/usr/bin/python3

import csv
import re
import sys
from urllib.parse import urlparse, urlunparse
from common import get_option, make_connection
from cursor_wrapper import CursorWrapper

status_rx = re.compile("^/([-\\w]+)/status/")

class Checker(CursorWrapper):
    def __init__(self, cur, mode):
        CursorWrapper.__init__(self, cur)
        if mode == 'maw':
            self.maw_threshold = int(get_option("maw_threshold", "10"))
            self.account2max = {}
            self.process = self.process_maw
            self.dump = self.dump_maw
        else:
            self.track = []
            self.process = self.process_default
            self.dump = self.dump_default

    def run(self, writer):
        writer.writerow(['url', 'count'])

        self.cur.execute("""select url, tt.c
from field
join (
    select count(from_id) as c, to_id
    from redirect
    group by to_id ) as tt on id=tt.to_id
order by tt.c desc""")
        rows = self.cur.fetchall()
        for row in rows:
            self.process(row[0], row[1])

        self.dump(writer)

    def process_default(self, url, cnt):
        self.track.append([url, cnt])

    def dump_default(self, writer):
        for row in self.track:
            writer.writerow(row)

    def process_maw(self, url, cnt):
        pr = urlparse(url)
        m = status_rx.match(pr.path)
        if not m:
            print("skipping redirect to " + url, file=sys.stderr)
            return

        town_name = m.group(1)
        account_pr = (pr.scheme, pr.netloc, town_name.lower(), '', '', '')
        account_url = urlunparse(account_pr)
        last_cnt = self.account2max.get(account_url, 0)
        if cnt > last_cnt:
            self.account2max[account_url] = cnt

    def dump_maw(self, writer):
        for url, cnt in sorted(self.account2max.items(), key=lambda p: (-1 * p[1], p[0])):
            if cnt < self.maw_threshold:
                return

            writer.writerow([url, cnt])


def main():
    mode = None
    l = len(sys.argv)
    if l == 2:
        if sys.argv[1] == '--maw':
            mode = 'maw'
        else:
            raise Exception("invalid argument")
    elif l > 2:
        raise Exception("too many arguments")

    with make_connection() as conn:
        with conn.cursor() as cur:
            checker = Checker(cur, mode)
            writer = csv.writer(sys.stdout, delimiter=",")
            checker.run(writer)


if __name__ == "__main__":
    main()
