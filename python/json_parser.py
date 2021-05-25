import json
import re
import sys
from common import get_option
from jumper import Jumper, make_meta_query_url
from leaf_merge import LeafMerger
from urlize import query_url_head

class JsonParser:
    core_url_head = "https://cro.justice.cz/verejnost/api/funkcionari"

    def __init__(self, owner, url):
        self.owner = owner
        self.leaf_merger = LeafMerger(owner.cur)
        self.entity_rx = re.compile('/(Q[0-9]+)$')
        self.jumper = None
        self.jump_links = int(get_option('jump_links', "1"))
        if (self.jump_links < 0) or (self.jump_links > 2):
            raise Exception("invalid option jump_links")

        schema = (
            ( "^" + re.escape(make_meta_query_url()) + '$', self.process_meta_query ),
            ( "^" + re.escape(query_url_head), self.drop_url ),
            ( "^" + self.core_url_head + "\\?order=DESC&page=(?P<page>\\d+)&pageSize=(?P<page_size>\\d+)&sort=created$", self.process_overview ),
            ( "^" + self.core_url_head + "/(?P<id>[0-9a-fA-F-]{36})$", self.process_detail )
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
            print("ignoring unknown URL", file=sys.stderr)
            return

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))

        self.process(doc)

    def drop_url(self, doc):
        pass

    def process_meta_query(self, doc):
        if self.jumper:
            raise Exception("filling jumper after it's been loaded")
        else:
            self.jumper = Jumper()

        bindings = doc['results']['bindings']
        for it in bindings:
            tdict = it.get('t')
            if tdict:
                m = self.entity_rx.search(tdict['value'])
                if m:
                    self.jumper.add_last_legislature(m.group(1))
                else:
                    raise Exception("unexpected result for last legislature: " + tdict['value'])
            else:
                m = self.entity_rx.search(it['q']['value'])
                if m:
                    self.jumper.add_muni_mayor(it['l']['value'], m.group(1))

        self.owner.set_jumper(self.jumper)

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

        person_id = self.match.group('id')
        self.leaf_merger.merge(person_id, doc)

        # enrich from Wikidata
        if not self.jumper:
            self.jumper = self.owner.get_jumper()
            if not self.jumper:
                # we could have failed to process the mayor-of URL;
                # that would be a problem, because we need the
                # information from it to link many following
                # URLs...
                raise Exception("jumper not set")

        position_set = self.jumper.make_position_set(doc)
        if len(position_set):
            urls = self.jumper.make_query_urls(doc, position_set)
            for url in urls:
                self.owner.add_link(url)

        if self.jump_links == 2:
            url = self.jumper.make_query_single_url(doc, set())
            self.owner.add_link(url)
