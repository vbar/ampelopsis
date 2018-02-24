#!/usr/bin/python3

import re
import sys
from urllib.parse import urlparse
from common import get_option, make_connection

def add_host(cur, hostname):
    cur.execute("""insert into tops(hostname) values(%s)
on conflict do nothing
returning hostname""", (hostname,))
    if cur.fetchone() is None:
        print("host %s already whitelisted" % (hostname,), file=sys.stderr)
    
def add_url(cur, url):
    cur.execute("""insert into field(url) values(%s)
on conflict do nothing
returning url""", (url,))
    if cur.fetchone() is None:
        print("URL %s already registered" % (url,), file=sys.stderr)

def add_work(cur, url, url_id):
    pr = urlparse(url)
    cur.execute("""insert into download_queue(url_id, priority, host_id)
values(%s, %s, (select id from tops where hostname=%s))
on conflict do nothing
returning url_id""", (url_id, 0, pr.netloc))
    if cur.fetchone() is None:
        print("URL %s already in queue" % (url_id,), file=sys.stderr)
    
def main():
    top_protocols = get_option('top_protocols', 'http')
    protocols = re.split('\\s+', top_protocols)
    with make_connection() as conn:
        with conn.cursor() as cur:
            for a in sys.argv[1:]:
                if a.startswith('http'):
                    pr = urlparse(a)
                    add_host(cur, pr.netloc)
                    add_url(cur, a)
                else:
                    add_host(cur, a)
                    for protocol in protocols:
                        add_url(cur, "%s://%s" % (protocol, a))

            cur.execute("""select url, id from field
where checkd is null
order by id""")
            rows = cur.fetchall()
            for row in rows:
                add_work(cur, *row)
                
if __name__ == "__main__":
    main()
    

