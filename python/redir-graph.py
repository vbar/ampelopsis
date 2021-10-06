#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import collections
import random
from urllib.parse import urlparse
from common import get_option, make_connection
from funnel_parser import status_rx
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase

class RefNet(PinholeBase):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.sample_max = int(get_option("redir_sample_max", "3"))
        self.vars2samples = collections.defaultdict(set) # pair of variants -> set of redirect sources

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
                m = status_rx.match(pr.path)
                if m:
                    town_name = m.group(1)
                    target_hamlet_name = self.town2hamlet.get(town_name)
                    if target_hamlet_name and (target_hamlet_name != source_hamlet_name):
                        self.do_load_item(town_url, source_hamlet_name, target_hamlet_name)

    def make_samples(self):
        sample_matrix = {}
        for variants, samples in self.vars2samples.items():
            source_name = self.get_presentation_name(variants[0])
            target_name = self.get_presentation_name(variants[1])
            sample_list = list(samples)
            if len(sample_list) > self.sample_max:
                random.shuffle(sample_list)

            row = sample_matrix.setdefault(source_name, {})
            row[target_name] = sample_list[0:self.sample_max]

        return sample_matrix

    def do_load_item(self, town_url, source_hamlet_name, target_hamlet_name):
        source_variant = self.get_variant(source_hamlet_name)
        if source_variant:
            target_variant = self.get_variant(target_hamlet_name)
            if target_variant:
                source_node = self.introduce_node(source_variant, self.distinguish)
                target_node = self.introduce_node(target_variant, False)
                edge = (source_node, target_node)
                weight = self.ref_map.get(edge, 0)
                self.ref_map[edge] = weight + 1

                edge_samples = self.vars2samples[(source_variant, target_variant)]
                edge_samples.add(town_url)


def main():
    ca = ConfigArgs()
    conn = make_connection()
    try:
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
    finally:
        conn.close()


if __name__ == "__main__":
    main()
