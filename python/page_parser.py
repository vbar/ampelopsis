import json
import re
import zipfile
from url_heads import hamlet_dump, hamlet_record_head

class PageParser:
    def __init__(self, owner, url):
        self.owner = owner

        schema = (
            ( "^" + hamlet_dump + "$", self.process_dump ),
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

        self.process(fp)

    def process_dump(self, fp):
        with zipfile.ZipFile(fp) as zp:
            info = zp.getinfo("dataset.veklep.dump.json")
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

        attachments = doc.get('prilohy')
        if isinstance(attachments, list):
            for att in attachments:
                att_url = att.get('DocumentUrl')
                if att_url:
                    self.owner.add_link(att_url)

    def get_doc(self, fp):
        buf = b''
        for ln in fp:
            buf += ln

        return json.loads(buf.decode('utf-8'))
