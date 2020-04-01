#!/usr/bin/python3

import collections
from lxml import etree
import re
import sys
from baker import make_meta_query_url, make_personage_query_urls
from common import get_option, make_connection
from json_frame import JsonFrame
from personage import parse_personage
from url_heads import green_url_head, town_url_head

PartySpec = collections.namedtuple('PartySpec', 'id_url long_name short_name color')

datetime_rx = re.compile('^([0-9]{4})-[0-9]{2}-[0-9]{2}T00:00:00Z$')

def get_opt(it, vn):
    d = it.get(vn)
    return d.get('value') if d else None


def birth_check(person, it):
    raw_date = get_opt(it, 'b')
    if not raw_date:
        # if the date isn't in response, it must have been filtered on
        # the server
        return True

    m = datetime_rx.match(raw_date)
    if not m:
        # shouldn't happen; in case of wikidata error, take the name
        # as sufficient
        return True

    year = int(m.group(1))
    return person.birth_year == year


class Condensator(JsonFrame):
    def __init__(self, cur):
        JsonFrame.__init__(self, cur)
        self.green_rx = re.compile("^" + green_url_head + "(?P<hname>[-a-zA-Z0-9]+)$")
        self.town_rx = re.compile("^" + town_url_head + "/(?P<tname>[^/]+)")

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s'
and checkd is not null
and url_id is null
order by url""" % green_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.process_page(*row)

    def process_page(self, card_url, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print(card_url + " not found on disk", file=sys.stderr)
            return

        try:
            self.condensate(card_url, reader)
        finally:
            reader.close()

    def condensate(self, card_url, fp):
        m = self.green_rx.match(card_url)
        if not m:
            print("malformed URL " + card_url, file=sys.stderr)
            return

        hamlet_name = m.group('hname')

        # no need to handle relative URLs - we're only interested in
        # the absolute one to Twitter
        context = etree.iterparse(fp, events=('end',), tag=('a', 'title'), html=True, recover=True)
        record_id = None
        for action, elem in context:
            if elem.tag == 'title':
                person = parse_personage(elem.text)
                if person:
                    record_id = self.condensate_record(person, hamlet_name)
                    if person.query_name:
                        self.condensate_party(record_id, person)
            elif elem.tag == 'a':
                href = elem.get('href')
                if href:
                    m = self.town_rx.match(href)
                    if m:
                        town_name = m.group('tname')
                        # social media links are in body, so person
                        # from title should be initialized by now
                        print("%s <=> %s" % (hamlet_name, town_name), file=sys.stderr)
                        self.insert_identity(record_id, town_name)

            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    def condensate_record(self, person, hamlet_name):
        self.cur.execute("""insert into vn_record(hamlet_name, presentation_name)
values(%s, %s)
on conflict do nothing
returning id""", (hamlet_name, person.presentation_name))
        row = self.cur.fetchone()
        if row is not None:
            return row[0]

        self.cur.execute("""select id
from vn_record
where hamlet_name=%s""", (hamlet_name,))
        row = self.cur.fetchone()
        return row[0]

    def condensate_party(self, record_id, person):
        name_rx = re.compile("\\b" + re.escape(person.query_name) + "\\b", re.IGNORECASE)
        query_urls = [ make_meta_query_url() ]
        query_urls.extend(make_personage_query_urls(person))

        wikidata_id = None
        party_spec = None
        for query_url in query_urls:
            query_id = self.get_url_id(query_url)
            if query_id:
                doc = self.get_document(query_id)
                if doc:
                    bindings = doc['results']['bindings']
                    for it in bindings:
                        if birth_check(person, it) and name_rx.search(it['l']['value']):
                            cur_id = it['w']['value']
                            if wikidata_id is None:
                                wikidata_id = cur_id
                            elif wikidata_id != cur_id:
                                print("query matches multiple persons", file=sys.stderr)
                                return

                            id_url = it['p']['value']
                            if party_spec is None:
                                party_spec = PartySpec(id_url=id_url, long_name=it['t']['value'],
                                        short_name=get_opt(it, 'z'), color=get_opt(it, 'c'))
                            elif party_spec.id_url != id_url:
                                print("person matches multiple parties", file=sys.stderr)
                                return

        if party_spec:
            party_id = self.insert_party(party_spec)
            self.update_record(record_id, party_id)

    def insert_identity(self, record_id, town_name):
        self.cur.execute("""insert into vn_identity_hamlet(record_id, town_name)
values(%s, %s)
on conflict do nothing""", (record_id, town_name))

    def insert_party(self, party_spec):
        self.cur.execute("""insert into vn_party(long_name, short_name, color)
values(%s, %s, %s)
on conflict(long_name) do update
set short_name=%s, color=%s
returning id""", (party_spec.long_name, party_spec.short_name, party_spec.color, party_spec.short_name, party_spec.color))
        row = self.cur.fetchone()
        if row is not None:
            return row[0]

        self.cur.execute("""select id
from vn_party
where long_name=%s""", (party_spec.long_name,))
        row = self.cur.fetchone()
        return row[0]

    def update_record(self, record_id, party_id):
        self.cur.execute("""update vn_record
set party_id=%s
where id=%s""", (party_id, record_id))

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            condensator = Condensator(cur)
            try:
                condensator.run()
            finally:
                condensator.close()


if __name__ == "__main__":
    main()
