#!/usr/bin/python3

import json
import sys
from common import get_option, make_connection
from show_room import ShowRoom
from token_util import tokenize

class Processor(ShowRoom):
    def __init__(self, cur):
        ShowRoom.__init__(self, cur)
        self.submitter2count = {}
        self.data = []

    def load_item(self, doc):
        dt = self.extend_date(doc)

        doc_url = doc.get('url')
        url = self.get_circuit_url(doc_url) if doc_url else "???"

        submitter = doc.get('predkladatel', '')
        cnt = self.submitter2count.get(submitter, 0)
        self.submitter2count[submitter] = 1 + cnt

        total = 0
        attachments = doc.get('prilohy')
        if isinstance(attachments, list):
            for att in attachments:
                txt = att.get('DocumentPlainText')
                if not txt:
                    length = 0
                else:
                    lst = tokenize(txt)
                    length = len(lst)

                total += length

        if total:
            item = [ url, dt, total, submitter ]
            self.data.append(item)

    def dump(self):
        submitter_keys = [ p[0] for p in sorted(self.submitter2count.items(), key=lambda q: (q[1], q[0])) ]
        indirect_submitter = {}
        submitters = []
        for idx, submitter in enumerate(submitter_keys):
            indirect_submitter[submitter] = idx
            submitters.append(submitter)

        data = []
        for url, dt, length, submitter in self.data:
            item = [ url, dt.isoformat(), length, indirect_submitter[submitter] ]
            data.append(item)

        custom = {
            'submitters': submitters,
            'data': data,
            'dateExtent': self.make_date_extent()
        }

        json.dump(custom, sys.stdout, indent=2, ensure_ascii=False)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            processor.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
