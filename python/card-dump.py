#!/usr/bin/python3

import json
from lxml import etree
import sys
from common import make_connection
from frame import Frame
from html_lookup import make_all_card_query_urls
from url_templates import speaker_rx
from urlize import print_query

class Lookup(Frame):
    def dump(self, url):
        if not speaker_rx.match(url):
            print(url + " is not a card", file=sys.stderr)
            return

        url_id = self.get_url_id(url)
        if not url_id:
            return

        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print(url + " not downloaded", file=sys.stderr)
            return None

        try:
            qurls = make_all_card_query_urls(url, reader)
            for qurl in qurls:
                print(qurl)
                print_query(qurl)
        finally:
            reader.close()


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            lookup = Lookup(cur)
            for a in sys.argv[1:]:
                lookup.dump(a)


if __name__ == "__main__":
    main()
