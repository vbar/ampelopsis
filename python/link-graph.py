#!/usr/bin/python3

# requires database filled by running condensate.py

from common import make_connection
from meme_base import MemeBase
from opt_util import get_quoted_list_option
from pinhole_args import ConfigArgs

class RefNet(MemeBase):
    def __init__(self, cur, distinguish, deconstructed):
        MemeBase.__init__(self, cur, '(https?://\\S+)', distinguish, deconstructed)

    def make_samples(self):
        sample_matrix = {}
        for variants, samples in self.vars2memes.items():
            source_name = self.get_presentation_name(variants[0])
            target_name = self.get_presentation_name(variants[1])
            sample_list = list(samples)
            row = sample_matrix.setdefault(source_name, {})
            row[target_name] = sample_list

        return sample_matrix


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
