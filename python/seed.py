#!/usr/bin/python3

import datetime
import re
import sys
from urllib.parse import urlencode, urlparse, urlunparse
from act_util import act_reset
from common import get_option, make_connection
from host_check import get_instance_id, make_canonicalizer

class Seeder:
    def __init__(self, cur):
        self.cur = cur
        self.canon = make_canonicalizer()
        self.inst_name = get_option("instance", None)
        self.inst_id = None

    def add_host(self, hostname):
        canon_host = self.canon.canonicalize_host(hostname)
        if not self.inst_name:
            self.cur.execute("""insert into tops(hostname)
values(%s)
on conflict do nothing
returning hostname""", (canon_host,))
            if self.cur.fetchone() is None:
                print("host %s already whitelisted" % (canon_host,), file=sys.stderr)
        else:
            if not self.inst_id:
                self.cur.execute("""insert into instances(instance_name)
values(%s)
on conflict do nothing
returning id""", (self.inst_name,))
                row = self.cur.fetchone()
                self.inst_id = row[0] if row else get_instance_id(self.cur, self.inst_name)

            self.cur.execute("""insert into tops(hostname, instance_id)
values(%s, %s)
on conflict do nothing
returning hostname""", (canon_host, self.inst_id))
            if self.cur.fetchone() is None:
                print("host %s already whitelisted" % (canon_host,), file=sys.stderr)

    def add_url(self, url):
        self.cur.execute("""insert into field(url)
values(%s)
on conflict do nothing
returning id""", (url,))
        row = self.cur.fetchone()
        if row is None:
            print("URL %s already registered" % (url,), file=sys.stderr)
        else:
            self.cur.execute("""insert into nodes(url_id, depth)
values(%s, 0)""", (row[0], ))

    def add_work(self, url, url_id):
        pr = urlparse(url)
        hostname = self.canon.canonicalize_host(pr.hostname)
        self.cur.execute("""insert into download_queue(url_id, priority, host_id)
values(%s, %s, (select id from tops where hostname=%s))
on conflict do nothing
returning url_id""", (url_id, 0, hostname))
        if self.cur.fetchone() is None:
            print("URL %s already in queue" % (url_id,), file=sys.stderr)

    def add_name(self, town_name):
        self.cur.execute("""insert into panel_names(town_name)
values(%s)
on conflict do nothing""", (town_name,))

    def seed_queue(self):
        self.cur.execute("""select url, id from field
where checkd is null
order by id""")
        rows = self.cur.fetchall()
        for row in rows:
            self.add_work(*row)


# search URL from twint (except with sorted query params)
class Formatter:
    def __init__(self, days_before):
        self.since = datetime.datetime.now() - datetime.timedelta(days=days_before)

    def format_urls(self, town_name):
        templ = [ "https", "twitter.com", "/i/search/timeline", "", "", "" ]
        urls = []

        for retweets in (False, True):
            templ[4] = self.format_params(town_name, retweets)
            urls.append(urlunparse(templ))

        return urls

    def format_params(self, town_name, retweets):
        params = [
            ('f', 'tweets'),
            ('include_available_features', '1'),
            ('include_entities', '1'),
            ('max_position', '-1')
        ]

        params.append(('q', self.format_query(town_name, retweets)))
        params.extend([
            ('reset_error_state', 'false'),
            ('src', 'unkn'),
            ('vertical', 'default'),
        ])

        return urlencode(params)

    def format_query(self, town_name, retweets):
        q = "from:%s since:%d" % (town_name, self.since.timestamp())
        if retweets:
            q += " filter:nativeretweets"

        return q


def main():
    top_protocols = get_option('top_protocols', 'http')
    protocols = re.split('\\s+', top_protocols)
    with make_connection() as conn:
        with conn.cursor() as cur:
            # maybe check here whether download and/or parse is running? it shouldn't...
            act_reset(cur)

            seeder = Seeder(cur)
            if (len(sys.argv) == 2) and (sys.argv[1] == "-t"):
                seeder.add_host("twitter.com")
                frm = Formatter(int(get_option("seed_days_before_now", "5")))
                for ln in sys.stdin:
                    raw_name = ln.rstrip()
                    if raw_name:
                        name = raw_name.lower()
                        seeder.add_name(name)
                        urls = frm.format_urls(name)
                        for url in urls:
                            seeder.add_url(url)
            else:
                for a in sys.argv[1:]:
                    if a.startswith('http'):
                        pr = urlparse(a)
                        seeder.add_host(pr.hostname)
                        seeder.add_url(a)
                    else:
                        seeder.add_host(a)
                        for protocol in protocols:
                            seeder.add_url("%s://%s" % (protocol, a))

            seeder.seed_queue()

if __name__ == "__main__":
    main()
