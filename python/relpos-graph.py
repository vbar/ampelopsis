#!/usr/bin/python3

# requires database filled by running condensate.py and downloaded
# data extended by running morphodita-stemmer.py

from common import make_connection
from opt_util import get_quoted_list_option
from pos_base import PosBase

class Processor(PosBase):
    def __init__(self, cur, deconstructed):
        PosBase.__init__(self, cur, deconstructed)

    def aggregate_values(self, variant, pos2payload):
        s = 0
        for pos, payload in pos2payload.items():
            s += payload.freq

        pos2frac = {}
        for pos, payload in pos2payload.items():
            pos2frac[pos] = payload.freq / s

        return pos2frac


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
