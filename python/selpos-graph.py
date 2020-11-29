#!/usr/bin/python3

# requires database filled by running condensate.py and downloaded
# data extended by running morphodita-stemmer.py

from common import make_connection
from pos_base import PosBase

class Processor(PosBase):
    def __init__(self, cur):
        PosBase.__init__(self, cur, '*')
        self.restrict_persons()

    def aggregate_values(self, variant, pos2payload):
        pos2freq = {}
        for pos, payload in pos2payload.items():
            pos2freq[pos] = payload.freq

        return pos2freq


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
