#!/usr/bin/python3

# requires database filled by running condensate.py

from common import make_connection
from meme_base import MemeBase
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs

class RefNet(MemeBase):
    def __init__(self, cur, distinguish, deconstructed):
        MemeBase.__init__(self, cur, '#([-\\w]+)', distinguish, deconstructed)

    def make_matrix_desc(self):
        matrix_desc = {}
        for variants, tags in self.vars2memes.items():
            source_name = self.get_presentation_name(variants[0])
            target_name = self.get_presentation_name(variants[1])
            tag_list = [ "#" + t for t in sorted(tags) ]
            row = matrix_desc.setdefault(source_name, {})
            row[target_name] = ", ".join(tag_list)

        return matrix_desc


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
