#!/usr/bin/python3

import json
import re
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper
from jump_util import make_search_url, make_query_url
from volume_holder import VolumeHolder

class JsonLookup(VolumeHolder, CursorWrapper):
    datetime_rx = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T00:00:00Z$')

    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

    def get_extras(self, first_name, last_name):
        url = make_query_url(first_name, last_name)
        doc = self.get_document(url)
        if doc:
            bindings = doc['results']['bindings']
            if len(bindings):
                m = self.datetime_rx.match(bindings[0]['b']['value'])
                return (m.group(1), bindings[0]['a']['value'])

        return None

    def get_document(self, url):
        url_id = self.get_url_id(url)
        if url_id is None:
            return None

        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        buf = b''
        try:
            for ln in reader:
                buf += ln
        finally:
            reader.close()

        return json.loads(buf.decode('utf-8'))

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            print("unknown URL " + url, file=sys.stderr)
            return None

        return row[0]

def main():
    if len(sys.argv) != 3:
        raise Exception("usage: " + sys.argv[0] + " firstname lastname")

    with make_connection() as conn:
        with conn.cursor() as cur:
            lookup = JsonLookup(cur)
            res = lookup.get_extras(sys.argv[1], sys.argv[2])
            if res:
                print(res)

if __name__ == "__main__":
    main()
