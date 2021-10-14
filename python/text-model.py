#!/usr/bin/python3

import json
import sys
from common import get_loose_path, get_option, make_connection
from show_cabinet import ShowCabinet

class Processor(ShowCabinet):
    def __init__(self, cur):
        ShowCabinet.__init__(self, cur)
        self.storage_alternative = get_option("storage_alternative", None)

    def load_item(self, att):
        assert self.storage_alternative
        url = att['DocumentUrl']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        txt = att.get('DocumentPlainText')
        if not txt:
            return

        loose_path = get_loose_path(url_id, alt_repre=self.storage_alternative)
        with open(loose_path, 'w') as f:
            f.write(txt)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            if not processor.storage_alternative:
                print("no storage alternative set", file=sys.stderr)
                return

            try:
                processor.run()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
