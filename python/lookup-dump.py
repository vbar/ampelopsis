#!/usr/bin/python3

import json
from lxml import etree
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper
from html_lookup import make_card_query_urls
from url_templates import speaker_rx
from urlize import print_query
from volume_holder import VolumeHolder

class Lookup(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

    def get_url_id(self, url):
        self.cur.execute("""select id, checkd
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            print("unknown URL " + url, file=sys.stderr)
            return None

        if row[1] is None:
            print("URL " + url + " not downloaded", file=sys.stderr)
            return None

        return row[0]

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
            qurls = make_card_query_urls(url, reader)
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
