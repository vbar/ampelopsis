#!/usr/bin/python3

import collections
from dateutil.parser import parse
import sys
from common import get_option, make_connection
from show_room import ShowRoom
from token_util import tokenize

DocMeta = collections.namedtuple('DocMeta', 'pid name description attachments')

class Processor(ShowRoom):
    def __init__(self, cur):
        ShowRoom.__init__(self, cur)
        self.submitter2count = {}
        self.att_count = 0
        self.doc2meta = {} # int url ID -> DocMeta
        self.data = []

    def load_item(self, doc):
        dt = self.extend_date(doc)

        doc_url = doc.get('url')
        doc_url_id = self.get_url_id(doc_url)

        submitter = doc.get('predkladatel', '')
        cnt = self.submitter2count.get(submitter, 0)
        self.submitter2count[submitter] = 1 + cnt

        attachments = doc.get('prilohy')
        if isinstance(attachments, list):
            self.att_count += len(attachments)
        else:
            attachments = None

        self.doc2meta[doc_url_id] = DocMeta(pid=doc.get('PID'), name=doc.get('nazevMaterialu'), description=doc.get('popis'), attachments=attachments)

        item = [ doc_url_id, dt, submitter ]
        self.data.append(item)

    def insert(self):
        print("inserting %d submitters..." % len(self.submitter2count.keys()))
        submitter_keys = [ p[0] for p in sorted(self.submitter2count.items(), key=lambda q: (-1 * q[1], q[0])) ]
        indirect_submitter = {}
        for submitter in submitter_keys:
            self.cur.execute("""insert into rap_submitters(submitter)
values(%s)
on conflict do nothing
returning id""", (submitter,))
            row = self.cur.fetchone()
            if not row:
                self.cur.execute("""select id
from rap_submitters
where submitter=%s""", (submitter,))
                row = self.cur.fetchone()

            indirect_submitter[submitter] = row[0]

        print("inserting %d documents..." % len(self.data))
        for url_id, dt, submitter in self.data:
            submitter_id = indirect_submitter[submitter]
            meta = self.doc2meta[url_id]
            self.cur.execute("""insert into rap_documents(doc_id, pid, submitter_id, doc_day, doc_name, doc_desc)
values(%s, %s, %s, %s, %s, %s)
on conflict(doc_id) do update
set pid=%s, submitter_id=%s, doc_day=%s, doc_name=%s, doc_desc=%s""", (url_id, meta.pid, submitter_id, dt, meta.name, meta.description, meta.pid, submitter_id, dt, meta.name, meta.description))

        print("inserting %d attachments..." % self.att_count)
        for doc_id, meta in sorted(self.doc2meta.items(), key=lambda p: p[0]):
            attachments = meta.attachments
            if attachments is not None:
                att_no = 0
                for att in attachments:
                    att_no += 1

                    att_url = att.get('DocumentUrl')
                    att_id = self.get_url_id(att_url)

                    att_day = None
                    raw_att_day = att.get('datumVlozeniPrilohy')
                    if raw_att_day:
                        att_day = parse(raw_att_day)

                    att_type = att.get('typ')

                    txt = att.get('DocumentPlainText')
                    if not txt:
                        length = 0
                    else:
                        lst = tokenize(txt)
                        length = len(lst)

                    self.cur.execute("""insert into rap_attachments(att_id, doc_id, att_day, att_type, att_no, word_count)
values(%s, %s, %s, %s, %s, %s)
on conflict(att_id) do update
set doc_id=%s, att_day=%s, att_type=%s, att_no=%s, word_count=%s""", (att_id, doc_id, att_day, att_type, att_no, length, doc_id, att_day, att_type, att_no, length))


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            processor.insert()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
