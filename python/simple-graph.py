#!/usr/bin/python3

import os
from common import get_loose_path, get_option, make_connection
from length_base import LengthBase

class Processor(LengthBase):
    def __init__(self, cur):
        LengthBase.__init__(self, cur)
        self.simple_repre = get_option("simple_representation", "simple")

    def get_length(self, att):
        url = att.get('DocumentUrl')
        if not url:
            return 0

        url_id = self.get_url_id(url)
        simple_path = get_loose_path(url_id, alt_repre=self.simple_repre)
        if not os.path.exists(simple_path):
            return 0

        statinfo = os.stat(simple_path)
        return statinfo.st_size


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            processor.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
