#!/usr/bin/python3

import csv
import re
import sys
from urllib.parse import urlparse, urlunparse
from common import get_option, make_connection
from cursor_wrapper import CursorWrapper
from show_case import ShowCase

status_rx = re.compile("^/([-\\w]+)/status/")

class Checker(CursorWrapper):
    def __init__(self, cur, maw_mode):
        CursorWrapper.__init__(self, cur)
        if maw_mode:
            self.maw_threshold = int(get_option("maw_threshold", "10"))
            self.account2max = {}
            self.process = self.process_maw
            self.dump_body = self.dump_maw
        else:
            self.track = []
            self.process = self.process_default
            self.dump_body = self.dump_default

    def run(self):
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

    def get_maw(self):
        maw = set()
        for url, cnt in sorted(self.account2max.items(), key=lambda p: (-1 * p[1], p[0])):
            if cnt >= self.maw_threshold:
                maw.add(url)
            else:
                return maw

        # won't get here in practice, but for completeness...
        return maw

    def dump(self, writer):
        writer.writerow(['url', 'count'])
        self.dump_body(writer)

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


class Position:
    def __init__(self, inside):
        if inside:
            self.stand = 0
            self.fall = 1
        else:
            self.stand = 1
            self.fall = 0

    def increment(self, inside):
        if inside:
            self.fall += 1
        else:
            self.stand += 1

    @property
    def total(self):
        return self.stand + self.fall

    @property
    def survival(self):
        return self.stand / self.total


class PitChecker(ShowCase):
    def __init__(self, cur, maw):
        ShowCase.__init__(self, cur)
        self.maw = maw
        self.account2mix = {} # profile URL -> Position

    def dump(self, writer):
        writer.writerow(['url', 'total', 'spam'])

        for url, pos in sorted(self.account2mix.items(), key=lambda p: (-1 * p[1].total, p[1].survival, p[0])):
            row = [ url, pos.total, pos.fall ]
            writer.writerow(row)

    def load_item(self, et):
        town_url = et.get('url')
        if town_url:
            self.cur.execute("""select f2.url
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.url=%s""", (town_url,))
            rows = self.cur.fetchall()
            for row in rows:
                self.process(town_url, row[0])

    def process(self, source_url, target_url):
        src_acct_url = self.get_account_url(source_url, False)
        tgt_acct_url = self.get_account_url(target_url, True)
        if not src_acct_url or not tgt_acct_url:
            print("skipping redirect from %s to %s" % (source_url, target_url), file=sys.stderr)
            return

        inside = tgt_acct_url in self.maw
        pos = self.account2mix.get(src_acct_url)
        if pos is None:
            self.account2mix[src_acct_url] = Position(inside)
        else:
            pos.increment(inside)

    def get_account_url(self, url, lowercase):
        pr = urlparse(url)
        m = status_rx.match(pr.path)
        if not m:
            return None

        town_name = m.group(1)
        name = town_name.lower() if lowercase else town_name
        account_pr = (pr.scheme, pr.netloc, name, '', '', '')
        return urlunparse(account_pr)


def main():
    maw_mode = False
    pit_mode = False
    l = len(sys.argv)
    if l == 2:
        raw_mode = sys.argv[1]
        if raw_mode == '--maw':
            maw_mode = True
        elif raw_mode == '--pit':
            maw_mode = True
            pit_mode = True
        else:
            raise Exception("invalid argument")
    elif l > 2:
        raise Exception("too many arguments")

    with make_connection() as conn:
        with conn.cursor() as cur:
            checker = Checker(cur, maw_mode)
            writer = csv.writer(sys.stdout, delimiter=",")
            checker.run()

            if not pit_mode:
                checker.dump(writer)
            else:
                pit_checker = PitChecker(cur, checker.get_maw())
                pit_checker.run()
                pit_checker.dump(writer)


if __name__ == "__main__":
    main()
