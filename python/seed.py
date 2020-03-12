#!/usr/bin/python3

import re
import sys
from urllib.parse import urlparse
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

    def seed_queue(self):
        self.cur.execute("""select url, id from field
where checkd is null
order by id""")
        rows = self.cur.fetchall()
        for row in rows:
            self.add_work(*row)

def main():
    top_protocols = get_option('top_protocols', 'http')
    protocols = re.split('\\s+', top_protocols)
    with make_connection() as conn:
        with conn.cursor() as cur:
            # maybe check here whether download and/or parse is running? it shouldn't...
            act_reset(cur)

            seeder = Seeder(cur)
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
