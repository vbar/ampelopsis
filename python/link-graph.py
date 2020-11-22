#!/usr/bin/python3

# requires database filled by running condensate.py

import collections
from dateutil.parser import parse
import re
from common import make_connection
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs
from pinhole_base import PinholeBase
from tag_mixin import TagMixin, TagOcc

class RefNet(PinholeBase, TagMixin):
    def __init__(self, cur, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        TagMixin.__init__(self)
        self.url_rx = re.compile('(https?://\\S+)')
        self.lazy_ref_map = self.do_ref_map

    def load_item(self, et):
        hamlet_name = et['osobaid']
        occ_date = parse(et['datum'])
        txt = et['text']
        for m in self.url_rx.finditer(txt):
            self.tag_line.append(TagOcc(tag=m.group(1), hamlet_name=hamlet_name, occ_date=occ_date))

    def make_samples(self):
        sample_matrix = {}
        for variants, samples in self.vars2tags.items():
            source_name = self.get_presentation_name(variants[0])
            target_name = self.get_presentation_name(variants[1])
            sample_list = list(samples)
            row = sample_matrix.setdefault(source_name, {})
            row[target_name] = sample_list

        return sample_matrix


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
