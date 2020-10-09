#!/usr/bin/python3

import json
import re
import sys
from cursor_wrapper import CursorWrapper
from jumper import Jumper
from pellet import Pellet
from volume_holder import VolumeHolder

# Multiple birth dates exist in wikidata (see e.g. Q12022907). We
# consider the date useable if either all birth dates, or at least all
# birth dates with day precision (dates with year precision are
# typically from Czech National Authority Database and suspect) agree
# on a year.
class BirthAggregator:
    def __init__(self):
        self.full = set() # int years
        self.exact = set() # int years

    def add_birth_year(self, pellet):
        year = pellet.get_birth_year()
        self.full.add(year)
        if pellet.is_birth_date_exact():
            self.exact.add(year)

    def has_single_birth_year(self):
        return (len(self.full) == 1) or (len(self.exact) == 1)


class JsonLookup(VolumeHolder, CursorWrapper, Jumper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        Jumper.__init__(self)
        self.load(cur)

    def get_entities(self, detail):
        try:
            pellets = self.get_pellets(detail)
        except:
            print("unparseable query result", file=sys.stderr)
            return []

        persons = set(p.wikidataId for p in pellets)
        return list(persons)

    def get_persons(self, detail):
        try:
            pellets = self.get_pellets(detail)
        except:
            print("unparseable query result", file=sys.stderr)
            return []

        wid2years = {} # str -> BirthAggregator
        for p in pellets:
            bagg = wid2years.get(p.wikidataId)
            if bagg is None:
                bagg = BirthAggregator()
                wid2years[p.wikidataId] = bagg

            bagg.add_birth_year(p)

        for wid, bagg in wid2years.items():
            if not bagg.has_single_birth_year():
                print("multiple birth years", file=sys.stderr)
                return []

        persons = set(p.wikidataId for p in pellets)
        return list(persons)

    def get_attributes(self, detail):
        pellets = self.get_pellets(detail)
        if len(pellets):
            # One attribute (aboutLink) can be preferred by sorting...
            pellets.sort(key=lambda p: p.get_key(), reverse=True)
            first = pellets[0]

            # ...but we also want to prefer exact birthDate, and for
            # that we may have to patch the returned pellet.
            bd = None
            prec = None
            for p in pellets:
                if p.is_birth_date_exact():
                    bd = p.birthDate
                    prec = p.datePrecision
            if bd:
                first.birthDate = bd
                first.datePrecision = prec

            return first
        else:
            return None

    def get_pellets(self, detail):
        pellets = []
        docs = self.get_query_documents(detail)
        for doc in docs:
            name_rx = self.make_name_rx(detail)
            bindings = doc['results']['bindings']
            for it in bindings:
                if name_rx.search(it['l']['value']):
                    m = Pellet.datetime_rx.match(it['b']['value'])
                    if m:
                        anode = it.get('a')
                        a = anode.get('value') if anode else None
                        n = int(it['n']['value'])
                        p = Pellet(it['w']['value'], m.group(1), n, a)
                        pellets.append(p)

        return pellets

    def make_name_rx(self, detail):
        names = self.make_person_names(detail)
        alts = [ "(" + re.escape(name) + ")" for name in names ]
        expr = '|'.join(alts)
        return re.compile("\\b" + expr + "\\b", re.IGNORECASE)

    # only matches w/ specific position(s)
    def get_query_documents(self, detail):
        position_set = self.make_position_set(detail)
        if not len(position_set):
            return []

        docs = []
        urls = self.make_query_urls(detail, position_set)
        for url in urls:
            docs.append(self.get_document(url))

        return docs

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
