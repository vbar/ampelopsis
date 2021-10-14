import json
import re
from url_heads import hamlet_record_head, hamlet_search_head

class PageParser:
    def __init__(self, owner, url):
        self.owner = owner

        schema = (
            ( "^" + hamlet_search_head + "\\?desc=1&strana=(?P<page>\\d+)&sort=Dne", self.process_overview ),
            ( "^" + hamlet_record_head, self.process_detail )
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

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))
        self.process(doc)

    def process_overview(self, doc):
        items = doc.get('results')
        page_size = len(items)
        page = int(self.match.group('page'))
        if (page == 1) and (page_size > 0):
            total = int(doc.get('total'))
            n = total // page_size

            # API has limit
            if n > 200:
                n = 200

            i = 2
            while i <= n:
                url = hamlet_search_head + ("?desc=1&strana=%d&sort=Dne" % i)
                self.owner.add_link(url)
                i += 1

        for et in items:
            record_id = et.get('Id')
            detail_url = hamlet_record_head + record_id
            self.owner.add_link(detail_url)

    def process_detail(self, doc):
        doc_url = doc.get('url')
        if doc_url:
            self.owner.add_link(doc_url)

        attachments = doc.get('prilohy')
        if isinstance(attachments, list):
            for att in attachments:
                att_url = att.get('DocumentUrl')
                if att_url:
                    self.owner.add_link(att_url)
