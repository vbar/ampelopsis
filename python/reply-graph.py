#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import sys
from common import make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase
from reply_mixin import ReplyMixin

class RefNet(PinholeBase, ReplyMixin):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        ReplyMixin.__init__(self)
        self.known = {} # int url id -> str hamlet name
        self.expected = {} # int url id -> set of str hamlet name

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
