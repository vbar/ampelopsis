#!/usr/bin/python3

# requires database filled by running condensate.py

import collections
from dateutil.parser import parse
import json
import re
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase
from url_heads import town_url_head

TagOcc = collections.namedtuple('TagOcc', 'tag hamlet_name occ_date')

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.tag_rx = re.compile('#([-\\w]+)')
        self.tag_line = [] # of TagOcc

    def load_item(self, et):
        hamlet_name = et['osobaid']
        occ_date = parse(et['datum'])
        txt = et['text']
        for m in self.tag_rx.finditer(txt):
            self.tag_line.append(TagOcc(tag=m.group(1), hamlet_name=hamlet_name, occ_date=occ_date))

    def lazy_ref_map(self):
        if len(self.ref_map):
            return

        tag_line = sorted(self.tag_line, key=lambda tgo: tgo.occ_date)
        topics = {} # tag => set of variant
        for tgo in tag_line:
            variant = self.get_variant(tgo.hamlet_name)
            if variant is not None:
                top = topics.get(tgo.tag)
                if top is None:
                    originator = set()
                    originator.add(variant)
                    topics[tgo.tag] = originator
                elif variant not in top:
                    target_node = self.introduce_node(variant, False)
                    for prev_variant in top:
                        source_node = self.introduce_node(prev_variant, self.distinguish)
                        edge = (source_node, target_node)
                        weight = self.ref_map.get(edge, 0)
                        self.ref_map[edge] = weight + 1

                    top.add(variant)


def main():
    ca = ConfigArgs()
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            ref_net = RefNet(cur, not ca.chord, parties)
            try:
                ref_net.run()
                if not ca.chord:
                    ref_net.dump_standard()
                else:
                    ref_net.dump_chord(ca.chord)
            finally:
                ref_net.close()


if __name__ == "__main__":
    main()
