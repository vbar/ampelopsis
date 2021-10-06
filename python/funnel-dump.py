#!/usr/bin/python3

# requires database filled by running condensate.py

import collections
from lxml import etree
import re
import sys
from baker import make_meta_query_url, make_personage_query_urls
from common import make_connection
from conden_util import birth_check, get_opt
from json_frame import JsonFrame
from personage import normalize_name, parse_personage

Card = collections.namedtuple('Card', 'presentation_name hamlet_name url_id')

class Dumper(JsonFrame):
    def __init__(self, cur, name):
        JsonFrame.__init__(self, cur)
        self.name = name
        self.cards = []
        self.init_cards()

    def init_cards(self):
        name = normalize_name(self.name)
        mask = '%' + name + '%'
        self.cur.execute("""select presentation_name, hamlet_name, card_url_id
from vn_record
where presentation_name ilike %s
and card_url_id is not null""", (mask,))
        rows = self.cur.fetchall()
        for row in rows:
            self.cards.append(Card(*row))

    def dump(self):
        print("found %d card(s) for %s" % (len(self.cards), self.name), file=sys.stderr)
        for card in self.cards:
            self.dump_card(card)

    def dump_card(self, card):
        print("%s\t%s" % (card.presentation_name, card.hamlet_name))

        url_id = card.url_id
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print("card not found on disk", file=sys.stderr)
            return

        try:
            self.read_card(url_id, reader)
        finally:
            reader.close()

    def read_card(self, card_url_id, fp):
        context = etree.iterparse(fp, events=('end',), tag=('a', 'title'), html=True, recover=True)
        record_id = None
        for action, elem in context:
            if elem.tag == 'title':
                person = parse_personage(elem.text)
                if person:
                    self.dump_person(person)

    def dump_person(self, person):
        name_rx = re.compile("\\b" + re.escape(person.query_name) + "\\b", re.IGNORECASE)
        gov_url = make_meta_query_url()
        query_urls = [ gov_url ]
        query_urls.extend(make_personage_query_urls(person))

        for query_url in query_urls:
            query_id = self.get_url_id(query_url)
            if query_id:
                doc = self.get_document(query_id)
                if doc:
                    print(query_url)
                    bindings = doc['results']['bindings']
                    for it in bindings:
                        if birth_check(person, it) and name_rx.search(it['l']['value']):
                            row = [ get_opt(it, n) or "" for n in ('w', 'l', 'p', 't', 'z', 'c') ]
                            print("\t".join(row))

                    print("")


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            for a in sys.argv[1:]:
                if a:
                    dumper = Dumper(cur, a)
                    dumper.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
