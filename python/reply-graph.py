#!/usr/bin/python3

# requires download with funnel_links set to 1 and database filled by
# running condensate.py

from lxml import etree
import re
import sys
from common import make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.html_parser = etree.HTMLParser()
        self.path_char_rx = re.compile("[^a-zA-Z0-9_./-]")
        self.known = {} # int url id -> str hamlet name
        self.expected = {} # int url id -> set of str hamlet name
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
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        target_hamlet_name = et['osobaid']
        self.add_known(url_id, target_hamlet_name)

        if self.is_redirected(url):
            return

        self.extend_date(et)
        root = self.get_html_document(url_id)
        ancestors = self.get_ancestors(root)
        for source_url_id in ancestors:
            source_hamlet_name = self.known.get(source_url_id)
            if source_hamlet_name is None:
                targets = self.expected.get(source_url_id)
                if targets is None:
                    self.expected[source_url_id] = set((target_hamlet_name,))
                else:
                    targets.add(target_hamlet_name)
            else:
                self.add_link(source_hamlet_name, target_hamlet_name)

    def dump_final_state(self):
        for url_id, targets in sorted(self.expected.items()):
            url = self.get_url(url_id)
            print(url, file=sys.stderr)
            for target_name in sorted(targets):
                print("\t" + target_name)

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

    def add_known(self, url_id, hamlet_name):
        if url_id in self.known:
            return

        self.known[url_id] = hamlet_name

        targets = self.expected.get(url_id)
        if not targets:
            return

        for target_name in targets:
            self.add_link(hamlet_name, target_name)

        del self.expected[url_id]

    def add_link(self, source_name, target_name):
        if source_name == target_name:
            return

        source_variant = self.get_variant(source_name)
        if not source_variant:
            return

        target_variant = self.get_variant(target_name)
        if not target_variant:
            return

        source_node = self.introduce_node(source_variant, self.distinguish)
        target_node = self.introduce_node(target_variant, False)
        edge = (source_node, target_node)
        weight = self.ref_map.get(edge, 0)
        self.ref_map[edge] = weight + 1


def main():
    ca = ConfigArgs()
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            ref_net = RefNet(cur, not ca.chord, parties)
            try:
                ref_net.run()
                ref_net.dump_final_state()
                if not ca.chord:
                    ref_net.dump_standard()
                else:
                    ref_net.dump_custom(ca.chord)
            finally:
                ref_net.close()


if __name__ == "__main__":
    main()
