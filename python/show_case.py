from dateutil.parser import parse
import os
import re
import sys
from common import get_loose_path, get_option
from json_frame import JsonFrame

class ShowCase(JsonFrame):
    def __init__(self, cur, silent=False):
        JsonFrame.__init__(self, cur)
        self.plain_repre = get_option("plain_repre", "plain")
        self.silent = silent
        self.mindate = None
        self.maxdate = None

    def run(self):
        self.cur.execute("""select url, id
from field
where url like 'http://localhost/%'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.load_detail(*row)

    def load_detail(self, speech_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(speech_url + " not found on disk", file=sys.stderr)
            return

        if not self.silent:
            print("loading %s..." % (speech_url,), file=sys.stderr)

        doc['url_id'] = url_id

        txt = None
        plain_path = get_loose_path(url_id, alt_repre=self.plain_repre)
        if os.path.exists(plain_path):
            with open(plain_path, 'r') as f:
                txt = f.read(txt)

        if txt:
            doc['text'] = txt

        self.load_item(doc)

    def extend_date(self, doc):
        d = doc.get('date')
        if not d:
            return None

        dt = parse(d)
        if (self.mindate is None) or (dt < self.mindate):
            self.mindate = dt

        if (self.maxdate is None) or (dt > self.maxdate):
            self.maxdate = dt

        return dt
