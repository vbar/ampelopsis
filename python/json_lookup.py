#!/usr/bin/python3

import json
import re
import sys
from urllib import parse
from common import make_connection
from cursor_wrapper import CursorWrapper
from jump_util import make_position_set, make_query_url
from volume_holder import VolumeHolder

class JsonLookup(VolumeHolder, CursorWrapper):
    datetime_rx = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T00:00:00Z$')

    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

    def get_entities(self, detail):
        doc = self.get_query_document(detail)
        persons = set()
        if doc:
            bindings = doc['results']['bindings']
            for it in bindings:
                persons.add(it['w']['value'])

        return list(persons)

    def get_attributes(self, detail):
        doc = self.get_query_document(detail)
        if doc:
            bindings = doc['results']['bindings']
            if len(bindings):
                m = self.datetime_rx.match(bindings[0]['b']['value'])
                return (m.group(1), bindings[0]['a']['value'])

        return None

    # only matches w/ specific position(s)
    def get_query_document(self, detail):
        position_set = make_position_set(detail)
        if not len(position_set):
            return None

        url = make_query_url(detail, position_set)
        return self.get_document(url)

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

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            lookup = JsonLookup(cur)
            for a in sys.argv[1:]:
                detail = lookup.get_document(a)
                position_set = make_position_set(detail)
                qurl = make_query_url(detail, position_set)
                uo = parse.urlparse(qurl)
                params = parse.parse_qsl(uo.query)
                for p in params:
                    if p[0] == 'query':
                        print(p[1])

                print("")
                leaf = lookup.get_document(qurl)
                if leaf:
                    json.dump(leaf, sys.stdout, ensure_ascii=False)
                    print("")

                if len(position_set):
                    qurl = make_query_url(detail, set())
                    leaf = lookup.get_document(qurl)
                    if leaf:
                        uo = parse.urlparse(qurl)
                        print("")
                        params = parse.parse_qsl(uo.query)
                        for p in params:
                            if p[0] == 'query':
                                print(p[1])

                        print("")
                        json.dump(leaf, sys.stdout, ensure_ascii=False)
                        print("")

if __name__ == "__main__":
    main()
