#!/usr/bin/python3

# requires database filled by running condensate.py

import re
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.nick_rx = re.compile('@([-\\w]+)')

    def load_item(self, et):
        self.extend_date(et)
        source_hamlet_name = et.get('osobaid')
        source_variant = self.get_variant(source_hamlet_name)
        if source_variant:
            self.do_load_item(et, source_hamlet_name, source_variant)

    def do_load_item(self, et, source_hamlet_name, source_variant):
        edge_set = set()
        txt = et.get('text')
        for m in self.nick_rx.finditer(txt):
            target_town_name = m.group(1)
            target_hamlet_name = self.town2hamlet.get(target_town_name)
            if target_hamlet_name and (target_hamlet_name != source_hamlet_name):
                target_variant = self.get_variant(target_hamlet_name)
                if target_variant:
                    source_node = self.introduce_node(source_variant, self.distinguish)
                    target_node = self.introduce_node(target_variant, False)
                    edge_set.add((source_node, target_node))

        for edge in edge_set:
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
                if not ca.chord:
                    ref_net.dump_standard()
                else:
                    ref_net.dump_custom(ca.chord)
            finally:
                ref_net.close()


if __name__ == "__main__":
    main()
