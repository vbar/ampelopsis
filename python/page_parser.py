import json
from lxml import etree
import re
import zipfile
from baker import make_personage_query_urls
from personage import parse_personage
from url_heads import green_url_head, hamlet_dump, hamlet_record_head

class PageParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.html_parser = etree.HTMLParser()

        schema = (
            ( "^" + hamlet_dump + "$", self.process_dump ),
            ( "^" + hamlet_record_head, self.process_detail ),
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
            # external URL
            return

        self.process(fp)

    def process_dump(self, fp):
        with zipfile.ZipFile(fp) as zp:
            info = zp.getinfo("dataset.stenozaznamy-psp.dump.json")
            with zp.open(info) as f:
                self.process_overview(f)

    def process_overview(self, fp):
        doc = self.get_doc(fp)
        for skel in doc:
            record_id = skel['id']
            detail_url = hamlet_record_head + record_id
            self.owner.add_link(detail_url)

    def process_detail(self, fp):
        doc = self.get_doc(fp)
        doc_url = doc.get('url')
        if doc_url:
            self.owner.add_link(doc_url)

        hamlet_name = doc.get('OsobaId')
        if hamlet_name:
            card_url = green_url_head + hamlet_name
            self.owner.add_link(card_url)

    def process_card(self, fp):
        # iterative parser no longer works (doesn't get header text)
        doc = etree.parse(fp, self.html_parser)
        person = None
        headers = doc.xpath("//h3")
        for header in headers:
            whole_text = "".join(header.xpath("text()"))
            person = parse_personage(whole_text)
            if person and person.query_name:
                wikidata_urls = make_personage_query_urls(person)
                for wikidata_url in wikidata_urls:
                    self.owner.add_link(wikidata_url)

    def get_doc(self, fp):
        buf = b''
        for ln in fp:
            buf += ln

        return json.loads(buf.decode('utf-8'))
