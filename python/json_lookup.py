#!/usr/bin/python3

import json
import re
import sys
from cursor_wrapper import CursorWrapper
from jumper import Jumper
from volume_holder import VolumeHolder

class JsonLookup(VolumeHolder, CursorWrapper, Jumper):
    datetime_rx = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})T00:00:00Z$')

    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        Jumper.__init__(self)
        self.load(cur)

    def get_entities(self, detail):
        doc = self.get_query_document(detail)
        persons = set()
        if doc:
            name_rx = self.make_name_rx(detail)
            bindings = doc['results']['bindings']
            for it in bindings:
                if name_rx.search(it['l']['value']):
                    persons.add(it['w']['value'])

        return list(persons)

    def get_attributes(self, detail):
        doc = self.get_query_document(detail)
        if doc:
            name_rx = self.make_name_rx(detail)
            bindings = doc['results']['bindings']
            for it in bindings:
                if name_rx.search(it['l']['value']):
                    m = self.datetime_rx.match(bindings[0]['b']['value'])
                    return (m.group(1), bindings[0]['a']['value'])

        return None

    def make_name_rx(self, detail):
        name = self.make_person_name(detail)
        return re.compile("\\b" + re.escape(name) + "\\b", re.IGNORECASE)

    # only matches w/ specific position(s)
    def get_query_document(self, detail):
        position_set = self.make_position_set(detail)
        if not len(position_set):
            return None

        url = self.make_query_url(detail, position_set)
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
