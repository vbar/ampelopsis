from lxml import etree
from urllib.parse import urljoin
import zipfile
from html_lookup import make_all_card_query_urls
from url_templates import legislature_index_rx, segment_local_rx, segment_rx, session_archive_rx, session_index_rx, session_page_rx, speaker_rx

class PageParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.orig_url = url
        self.base = url
        self.found_base = False

        schema = (
            ( legislature_index_rx, self.process_legislature ),
            ( session_archive_rx, self.process_archive ),
            ( session_index_rx, self.process_session ),
            ( session_page_rx, self.process_page ),
            ( segment_rx, self.process_segment ),
            ( speaker_rx, self.process_card )
        )

        self.match = None
        for url_rx, proc_meth in schema:
            m = url_rx.match(url)
            if m:
                self.match = m
                self.process = proc_meth
                break

    def parse_links(self, fp):
        if not self.match:
            # speaker/external page
            return

        self.process(fp)

    def process_legislature(self, fp):
        self.process_html(fp, session_archive_rx)

    def process_archive(self, fp):
        with zipfile.ZipFile(fp) as zp:
            for info in zp.infolist():
                if segment_local_rx.match(info.filename):
                    with zp.open(info) as f:
                        self.process_segment(f)

    def process_session(self, fp):
        self.process_html(fp, session_page_rx)

    def process_page(self, fp):
        self.process_html(fp, segment_rx)

    def process_segment(self, fp):
        self.process_html(fp, speaker_rx)

    def process_card(self, fp):
        wikidata_urls = make_all_card_query_urls(self.orig_url, fp)
        for wikidata_url in wikidata_urls:
            self.owner.add_link(wikidata_url)

    def process_html(self, fp, child_rx):
        context = etree.iterparse(fp, events=('end',), tag=('a', 'base'), html=True, recover=True)
        for action, elem in context:
            if not self.found_base and (elem.tag == 'base'):
                parent = elem.getparent()[0]
                if parent is not None and (parent.tag == 'head'):
                    grandparent = parent.getparent()[0]
                    if grandparent is not None and (grandparent.tag == 'html'):
                        self.found_base = True
                        href = elem.get('href')
                        if href:
                            self.base = urljoin(self.base, href)
            elif elem.tag == 'a':
                href = elem.get('href')
                if href:
                    link = urljoin(self.base, href)
                    if child_rx.match(link):
                        self.owner.add_link(link)

            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
