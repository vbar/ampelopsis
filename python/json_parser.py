import json
import re
import sys
from common import get_option
from jump_util import make_position_set, make_query_url

class JsonParser:
    core_url_head = "https://cro.justice.cz/verejnost/api/funkcionari"

    def __init__(self, owner, url):
        self.owner = owner
        self.jump_links = int(get_option('jump_links', "1"))
        if (self.jump_links < 0) or (self.jump_links > 2):
            raise Exception("invalid option jump_links")

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
        if not self.jump_links:
            return

        # enrich from Wikidata
        position_set = make_position_set(doc)
        if len(position_set):
            url = make_query_url(doc, position_set)
            self.owner.add_link(url)

        if self.jump_links == 2:
            url = make_query_url(doc, set())
            self.owner.add_link(url)
