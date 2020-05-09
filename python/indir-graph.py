#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import collections
from dateutil.parser import parse
import re
from common import make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase

OrigOcc = collections.namedtuple('OrigOcc', 'orig_url hamlet_name occ_date')

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.orig_line = [] # of OrigOcc

    def load_item(self, et):
        self.extend_date(et)
        hamlet_name = et['osobaid']
        occ_date = parse(et['datum'])
        town_url = et.get('url')
        if town_url:
            self.cur.execute("""select f2.url
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.url=%s""", (town_url,))
            rows = self.cur.fetchall()
            for row in rows:
                self.orig_line.append(OrigOcc(orig_url=row[0], hamlet_name=hamlet_name, occ_date=occ_date))

    def lazy_ref_map(self):
        if len(self.ref_map):
            return

        orig_line = sorted(self.orig_line, key=lambda ogo: ogo.occ_date)
        repeats = {} # orig url => set of variant
        for ogo in orig_line:
            variant = self.get_variant(ogo.hamlet_name)
            if variant is not None:
                top = repeats.get(ogo.orig_url)
                if top is None:
                    originator = set()
                    originator.add(variant)
                    repeats[ogo.orig_url] = originator
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
                    ref_net.dump_custom(ca.chord)
            finally:
                ref_net.close()


if __name__ == "__main__":
    main()
