#!/usr/bin/python3

# requires download with funnel_links set to 1 and database filled by
# running condensate.py

import re
from urllib.parse import urlparse
from common import make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.status_rx = re.compile("/([-\\w]+)/status/")

    def load_item(self, et):
        self.extend_date(et)
        source_hamlet_name = et['osobaid']
        town_url = et.get('url')
        if town_url:
            self.cur.execute("""select f2.url
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.url=%s""", (town_url,))
            rows = self.cur.fetchall()
            for row in rows:
                pr = urlparse(row[0])
                m = self.status_rx.match(pr.path)
                if m:
                    town_name = m.group(1)
                    target_hamlet_name = self.town2hamlet.get(town_name)
                    if target_hamlet_name and (target_hamlet_name != source_hamlet_name):
                        self.do_load_item(source_hamlet_name, target_hamlet_name)

    def do_load_item(self, source_hamlet_name, target_hamlet_name):
        source_variant = self.get_variant(source_hamlet_name)
        if source_variant:
            target_variant = self.get_variant(target_hamlet_name)
            if target_variant:
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
                if not ca.chord:
                    ref_net.dump_standard()
                else:
                    ref_net.dump_custom(ca.chord)
            finally:
                ref_net.close()


if __name__ == "__main__":
    main()
