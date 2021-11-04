#!/usr/bin/python3

import json
import sys
from common import get_loose_path, get_option, make_connection
from show_case import ShowCase

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.plain_alt = get_option("storage_alternative", "plain")

    def load_item(self, rec):
        url_id = rec['url_id']
        txt = rec.get('text')
        if txt:
            self.write_alt(url_id, txt)

    def write_alt(self, url_id, txt):
        loose_path = get_loose_path(url_id, alt_repre=self.plain_alt)
        with open(loose_path, 'w') as f:
            f.write(txt)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
