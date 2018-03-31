#!/usr/bin/python3

import re
import sys
from urllib.parse import urlparse
from common import get_option, make_connection
from host_check import make_canonicalizer

class Seeder:
    def __init__(self, cur):
        self.cur = cur
        self.canon = make_canonicalizer()
        
    def add_host(self, hostname):
        canon_host = self.canon.canonicalize_host(hostname)
        self.cur.execute("""insert into tops(hostname) values(%s)
on conflict do nothing
returning hostname""", (canon_host,))
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
    
def main():
    top_protocols = get_option('top_protocols', 'http')
    protocols = re.split('\\s+', top_protocols)
    with make_connection() as conn:
        with conn.cursor() as cur:
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

            cur.execute("""select url, id from field
where checkd is null
order by id""")
            rows = cur.fetchall()
            for row in rows:
                seeder.add_work(*row)
                
if __name__ == "__main__":
    main()


