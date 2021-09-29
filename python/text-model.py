#!/usr/bin/python3

import json
import sys
from common import get_loose_path, get_option, make_connection
from show_case import ShowCase

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.storage_alternative = get_option("storage_alternative", None)

    def load_item(self, et):
        assert self.storage_alternative
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        loose_path = get_loose_path(url_id, alt_repre=self.storage_alternative)
        with open(loose_path, 'w') as f:
            json.dump(et, f, indent=2, ensure_ascii=False)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            if not processor.storage_alternative:
                print("no storage alternative set", file=sys.stderr)
                return

            try:
                processor.run()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
