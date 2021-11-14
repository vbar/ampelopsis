from lxml import etree
from urllib.parse import urljoin
from url_templates import legislature_index_rx, segment_rx, session_archive_rx, session_index_rx, session_page_rx


class PageParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.base = url
        self.found_base = False

        schema = (
            ( legislature_index_rx, self.process_legislature ),
            ( session_index_rx, self.process_session ),
            ( session_page_rx, self.process_page )
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
            # archive or session page
            return

        self.process(fp)

    def process_legislature(self, fp):
        self.process_index(fp, session_archive_rx)

    def process_session(self, fp):
        self.process_index(fp, session_page_rx)

    def process_page(self, fp):
        self.process_index(fp, segment_rx)

    def process_index(self, fp, child_rx):
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
