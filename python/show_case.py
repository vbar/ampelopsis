from dateutil.parser import parse
import re
import sys
from common import get_option
from json_frame import JsonFrame
from opt_util import get_quoted_list_option
from url_heads import hamlet_url_head

class ShowCase(JsonFrame):
    def __init__(self, cur, silent=False):
        JsonFrame.__init__(self, cur)
        self.silent = silent
        self.mindate = None
        self.maxdate = None
        self.short_circuit_template = get_option("short_circuit_template", None)
        self.selected_tag_rx = None
        selected_tags = get_quoted_list_option("selected_tags", None)
        if selected_tags is not None:
            plain_gen = (t[1:] if t[0] == '#' else t for t in selected_tags if t)
            plain_set = set((t.lower() for t in plain_gen if t))
            quoted_list = [ re.escape(t) for t in sorted(plain_set) ]
            if len(quoted_list):
                subrx = '|'.join(quoted_list)
                self.selected_tag_rx = re.compile("#(%s)\\b" % subrx, re.I)

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
            if (self.selected_tag_rx is None) or self.selected_tag_rx.search(et['text']):
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

    def get_circuit_url(self, et):
        url = et['url']
        if not self.short_circuit_template:
            return url
        else:
            url_id = self.get_url_id(url)
            if url_id is None:
                return url
            else:
                return self.short_circuit_template.format(url_id)
