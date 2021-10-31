from dateutil.parser import parse
import re
import sys
from common import get_option
from json_frame import JsonFrame
from opt_util import get_quoted_list_option
from url_heads import hamlet_record_head

class ShowCase(JsonFrame):
    def __init__(self, cur, silent=False):
        JsonFrame.__init__(self, cur)
        self.silent = silent
        self.mindate = None
        self.maxdate = None
        self.short_circuit_template = get_option("short_circuit_template", None)

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s'
and checkd is not null
and url_id is null
order by url""" % hamlet_record_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.load_detail(*row)

    def load_detail(self, page_url, url_id):
        rec = self.get_document(url_id)
        if not rec:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        if not self.silent:
            print("loading %s..." % (page_url,), file=sys.stderr)

        rec['url_id'] = url_id
        self.load_item(rec)

    def is_redirected(self, url):
        self.cur.execute("""select count(*)
from field
join redirect on id=from_id
where url=%s""", (url,))
        row = self.cur.fetchone()
        return row[0] > 0

    def extend_date(self, rec):
        d = rec.get('datum')
        if not d:
            return None

        dt = parse(d)
        if (self.mindate is None) or (dt < self.mindate):
            self.mindate = dt

        if (self.maxdate is None) or (dt > self.maxdate):
            self.maxdate = dt

        return dt

    def get_circuit_url(self, ext_rec):
        if not self.short_circuit_template:
            return ext_rec['url']
        else:
            url_id = ext_rec['url_id']
            return self.short_circuit_template.format(url_id)

    def ensure_url_id(self, url):
        # simpler than download updates because it isn't safe for
        # parallel instances
        self.cur.execute("""insert into field(url, checkd, parsed)
values(%s, localtimestamp, localtimestamp)
on conflict(url) do nothing
returning id""", (url,))
        row = self.cur.fetchone()
        if row:
            return row[0]

        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        return row[0]
