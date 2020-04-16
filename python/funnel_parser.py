import json
from lxml import etree
import re
import sys
from baker import make_personage_query_urls
from common import get_option
from personage import parse_personage
from url_heads import green_url_head, hamlet_url_head, town_url_head

class FunnelParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.page_url = url
        self.funnel_links = int(get_option('funnel_links', "0"))
        if (self.funnel_links < 0) or (self.funnel_links > 1):
            raise Exception("invalid option funnel_links")

        schema = (
            ( "^" + hamlet_url_head + "\\?desc=1&page=(?P<page>\\d+)&q=server%3ATwitter&sort=datum$", self.process_overview ),
            ( "^" + green_url_head + "(?P<hname>[-a-zA-Z0-9]+)$", self.process_card )
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
            # wikidata queries
            return

        self.process(fp)

    def process_overview(self, fp):
        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))

        items = doc.get('results')
        page_size = len(items)
        page = int(self.match.group('page'))
        if (page == 1) and (page_size > 0):
            total = int(doc.get('total'))
            n = total // page_size
            i = 2
            while i <= n:
                url = hamlet_url_head + ("?desc=1&page=%d&q=server%%3ATwitter&sort=datum" % i)
                self.owner.add_link(url)
                i += 1

        for et in items:
            hamlet_name = et.get('osobaid')
            card_url = green_url_head + hamlet_name
            self.owner.add_link(card_url)

            if self.funnel_links:
                town_url = et.get('url')
                if town_url:
                    self.owner.add_link(town_url)

    def process_card(self, fp):
        context = etree.iterparse(fp, events=('end',), tag=('title'), html=True, recover=True)
        person = None
        for action, elem in context:
            assert elem.tag == 'title'
            person = parse_personage(elem.text)
            if person and person.query_name:
                wikidata_urls = make_personage_query_urls(person)
                for wikidata_url in wikidata_urls:
                    self.owner.add_link(wikidata_url)

            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
