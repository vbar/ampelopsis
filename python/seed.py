#!/usr/bin/python3

import re
import sys
from urllib.parse import urlparse
from act_util import act_reset
from common import get_option, make_connection
from host_check import get_instance_id, HostCheck

class Seeder:
    def __init__(self, cur):
        self.cur = cur
        self.host_check = None # initialized lazily, after this class updates tops
        if get_option("match_domain", False):
            raise Exception("domain canonicalizer isn't compatible with synthetic host")

        self.inst_name = get_option("instance", None)
        self.inst_id = None
        self.extra_header = get_option('extra_header', None)

    def add_host(self, canon_host):
        self.cur.execute("""insert into tops(hostname)
values(%s)
on conflict do nothing
returning id""", (canon_host,))
        row = self.cur.fetchone()
        if row is None:
            print("host %s already whitelisted" % (canon_host,), file=sys.stderr)

        if self.inst_name:
            if row is None:
                self.cur.execute("""select id
from tops
where hostname=%s""", (canon_host,))
                row = self.cur.fetchone()

            host_id = row[0]

            if not self.inst_id:
                self.do_add_instance()

            self.cur.execute("""insert into host_inst(host_id, instance_id)
values(%s, %s)
on conflict do nothing""", (host_id, self.inst_id))

    def add_synth_host(self, pr):
        if pr.hostname != 'www.hlidacstatu.cz':
            self.add_host(pr.hostname)
            return

        synth_host = pr.hostname
        segments = pr.path.split('/')
        private_path_flag = (len(segments) > 1) and (segments[1] == 'api')
        if self.extra_header is not None:
            if private_path_flag:
                synth_host = 'api.hlidacstatu.cz'
            else:
                print("will send private key to public pages", file=sys.stderr)
        else:
            if private_path_flag:
                raise Exception("private API requires configured private key")

        self.add_host(synth_host)

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
        if not self.host_check:
            self.host_check = HostCheck(self.cur)

        pr = urlparse(url)
        host_id = self.host_check.get_synth_host_id(pr)
        if not host_id:
            raise Exception("internal error: no host for " + url)

        self.cur.execute("""insert into download_queue(url_id, priority, host_id)
values(%s, %s, %s)
on conflict do nothing
returning url_id""", (url_id, 0, host_id))
        if self.cur.fetchone() is None:
            print("URL %s already in queue" % (url_id,), file=sys.stderr)

    def cond_add_instance(self):
        if self.inst_name and not self.inst_id:
            self.do_add_instance()

    def seed_queue(self):
        self.cur.execute("""select url, id from field
where checkd is null
order by id""")
        rows = self.cur.fetchall()
        for row in rows:
            self.add_work(*row)

    def do_add_instance(self):
        self.cur.execute("""insert into instances(instance_name)
values(%s)
on conflict do nothing
returning id""", (self.inst_name,))
        row = self.cur.fetchone()
        self.inst_id = row[0] if row else get_instance_id(self.cur, self.inst_name)

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
                    seeder.add_synth_host(pr)
                    seeder.add_url(a)
                else:
                    seeder.add_host(a)
                    for protocol in protocols:
                        seeder.add_url("%s://%s" % (protocol, a))

            seeder.cond_add_instance() # for seeding w/o arguments
            seeder.seed_queue()

if __name__ == "__main__":
    main()
