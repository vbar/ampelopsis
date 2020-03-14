import json
import re
import sys
from urllib.parse import urlparse
from common import get_option

class JsonParser:
    hamlet_url_head = "https://www.hlidacstatu.cz/api/v1/DatasetSearch/vyjadreni-politiku"

    def __init__(self, owner, url):
        self.owner = owner

        schema = (
            ( "^" + self.hamlet_url_head + "\\?desc=1&page=(?P<page>\\d+)&q=server%3ATwitter&sort=datum$", self.process_overview ),
        )

        self.match = None
        for url_rx, proc_meth in schema:
            m = re.match(url_rx, url)
            if m:
                self.match = m
                self.process = proc_meth
                break

    def parse_links(self, fp):
        if not self.match:
            return

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))
        self.process(doc)

    def process_overview(self, doc):
        page = int(self.match.group('page'))
        if page == 1:
            n = 20 # 1000 samples for a start
            i = 2
            while i <= n:
                url = self.hamlet_url_head + ("?desc=1&page=%d&q=server%%3ATwitter&sort=datum" % i)
                self.owner.add_link(url)
                i += 1

        items = doc.get('results')
        for et in items:
            hamlet_name = et.get('osobaid')
            url = et.get('url')
            pr = urlparse(url)
            segments = pr.path.split('/')
            town_name = segments[1]
            if town_name == 'undefined':
                self.owner.add_link(url)
            else:
                self.add_identity(hamlet_name, town_name)

    def add_identity(self, hamlet_name, town_name):
        cur = self.owner.cur
        cur.execute("""insert into vn_identity(hamlet_name, town_name, claim_count)
values(%s, %s, 1)
on conflict(hamlet_name, town_name) do update
set claim_count=vn_identity.claim_count + 1""", (hamlet_name, town_name))
