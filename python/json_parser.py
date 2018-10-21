import json
import re
import sys
from jump_util import make_query_url

class JsonParser:
    core_url_head = "https://cro.justice.cz/verejnost/api/funkcionari"

    wid_rx = re.compile('^Q[0-9]+$')

    def __init__(self, owner, url):
        self.owner = owner

        schema = (
            ( "^" + self.core_url_head + "\\?order=DESC&page=(?P<page>\\d+)&pageSize=(?P<page_size>\\d+)&sort=created$", self.process_overview ),
            ( "^" + self.core_url_head + "/[0-9a-fA-F-]+$", self.process_detail )
        )

        self.match = None
        for url_rx, proc_meth in schema:
            m = re.match(url_rx, url)
            if m:
                self.match = m
                self.process = proc_meth
                break

        if not self.match:
            print("skipping %s with unknown schema" % url, file=sys.stderr)

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
        page_size = int(self.match.group('page_size'))
        if page == 0:
            count = int(doc.get('count'))
            n = count // page_size
            i = 1
            while i <= n:
                url = self.core_url_head + ("?order=DESC&page=%d&pageSize=%d&sort=created" % (i, page_size))
                self.owner.add_link(url)
                i += 1

        items = doc.get('items')
        for person in items:
            person_id = person.get('id')
            url = self.core_url_head + ("/%s" % person_id)
            self.owner.add_link(url)

    def process_detail(self, doc):
        # enrich from Wikidata
        # it would be a bit simpler to call this from
        # process_overview, but JsonConvert gets the name from detail,
        # and in overview it isn't always the same
        url = make_query_url(doc.get('firstName'), doc.get('lastName'))
        self.owner.add_link(url)
