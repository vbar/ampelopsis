from lxml import etree
import re
import sys

class ReplyMixin: # CursorWrapper & VolumeHolder interfaces must be
                  # provided by another inherited class
    def __init__(self):
        self.html_parser = etree.HTMLParser()
        self.path_char_rx = re.compile("[^a-zA-Z0-9_./-]")
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

    def get_html_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        try:
            return etree.parse(reader, self.html_parser)
        finally:
            reader.close()

    def get_ancestors(self, root):
        ancestors = set()
        if not root:
            return ancestors

        attrs = root.xpath("//div[@data-component-term='in_reply_to']//div/@data-permalink-path")
        for a in attrs:
            path = self.path_char_rx.sub("", a)
            # using reverse index speeds up the script more than 5 times
            lpath = path.lower()
            mask = lpath[::-1] + '%'
            self.cur.execute("""select url_id
from vn_reverse_field
where reverse_lowercase like %s""", (mask,))
            rows = self.cur.fetchall()
            for row in rows:
                ancestors.add(row[0])

        return ancestors

    def get_table_count(self, table_name):
        self.cur.execute("""select count(*)
from %s""" % table_name)
        row = self.cur.fetchone()
        return row[0]
