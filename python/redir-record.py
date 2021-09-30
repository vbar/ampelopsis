#!/usr/bin/python3

from dateutil.parser import parse
import sys
from common import make_connection
from show_case import ShowCase

# unlike the Extender in redir-extend.py, this class doesn't add
# tweets of unknown persons
class Extender(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.tangle = {} # str Twitter ID -> int field URL ID -> datetime date

    def load_item(self, et):
        url_id = self.ensure_url_id(et['url'])

        origid = et.get('origid')
        if not origid:
            return

        url = et['url']
        if url.endswith("/" + origid):
            id2date = self.tangle.setdefault(origid, {})
            id2date[url_id] = parse(et['datum'])


class Processor(ShowCase):
    def __init__(self, cur, tangle):
        ShowCase.__init__(self, cur, tangle)
        self.tangle = tangle
        self.ensure_reverse()

    def ensure_reverse(self):
        fcnt = self.get_table_count('field')
        rcnt = self.get_table_count('vn_reverse_field')
        if fcnt == rcnt:
            return

        print("indexing URLs in reverse...", file=sys.stderr)
        if rcnt > 0:
            self.cur.execute("""delete from vn_reverse_field""")

        self.cur.execute("""insert into vn_reverse_field(reverse_lowercase, url_id)
select reverse(lower(url)), id
from field""")

    def load_item(self, et):
        origid = et.get('origid')
        if not origid:
            return

        id2date = self.tangle.get(origid)
        if id2date:
            line = sorted(id2date.keys(), key=lambda i: id2date[i])
            target_id = line.pop(0)
            for source_id in line:
                self.add_redirect(source_id, target_id)
        else:
            source_url = et['url']
            source_id = self.get_url_id(source_url)
            tail = "/status/" + origid.lower()
            mask = tail[::-1] + '%'
            self.cur.execute("""select url_id
from vn_reverse_field
where reverse_lowercase like %s""", (mask,))
            rows = self.cur.fetchall()
            for row in rows:
                self.add_redirect(source_id, row[0])

    def get_table_count(self, table_name):
        self.cur.execute("""select count(*)
from %s""" % table_name)
        row = self.cur.fetchone()
        return row[0]

    def add_redirect(self, source_url_id, target_url_id):
        if source_url_id != target_url_id:
            self.cur.execute("""insert into redirect(from_id, to_id)
values(%s, %s)
on conflict do nothing""", (source_url_id, target_url_id))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            ext = Extender(cur)
            ext.run()
            proc = Processor(cur, ext.tangle)
            proc.run()


if __name__ == "__main__":
    main()
