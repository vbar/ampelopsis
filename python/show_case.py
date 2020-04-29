from dateutil.parser import parse
import sys
from json_frame import JsonFrame
from url_heads import hamlet_url_head

class ShowCase(JsonFrame):
    def __init__(self, cur, silent=False):
        JsonFrame.__init__(self, cur)
        self.silent = silent
        self.mindate = None
        self.maxdate = None

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s'
and checkd is not null
and url_id is null
order by url""" % hamlet_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.load_page(*row)

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        if not self.silent:
            print("loading %s..." % (page_url,), file=sys.stderr)

        items = doc.get('results')
        for et in items:
            self.load_item(et)

    def is_redirected(self, url):
        self.cur.execute("""select count(*)
from field
join redirect on id=from_id
where url=%s""", (url,))
        row = self.cur.fetchone()
        return row[0] > 0

    def extend_date(self, et):
        dt = parse(et['datum'])
        if (self.mindate is None) or (dt < self.mindate):
            self.mindate = dt

        if (self.maxdate is None) or (dt > self.maxdate):
            self.maxdate = dt

        return dt
