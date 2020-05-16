#!/usr/bin/python3

import collections
from lxml import etree
import re
import sys
from urllib.parse import urlparse
from baker import make_meta_query_url, make_personage_query_urls
from common import get_option, make_connection
from conden_util import birth_check, get_from_year, get_opt
from json_frame import JsonFrame
from personage import parse_personage
from url_heads import green_url_head, short_town_url_head, town_url_head
from urlize import whitespace_rx

PartySpec = collections.namedtuple('PartySpec', 'id_url long_name short_name color from_year')

class Condensator(JsonFrame):
    def __init__(self, cur):
        JsonFrame.__init__(self, cur)
        self.green_rx = re.compile("^" + green_url_head + "(?P<hname>[-a-zA-Z0-9]+)$")
        self.town_rx = re.compile("^" + town_url_head + "/(?P<tname>[^/]+)")

        self.funnel_links = int(get_option('funnel_links', "0"))
        if (self.funnel_links < 0) or (self.funnel_links > 3):
            raise Exception("invalid option funnel_links")

        if self.funnel_links >= 2:
            self.rec2qname = {} # vn_record ID => personage query name

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

        if self.funnel_links >= 2:
            self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s/[^/]+/?$'
and checkd is not null
and url_id is null
order by url""" % short_town_url_head)
            rows = self.cur.fetchall()
            for row in rows:
                self.process_profile(*row)

    def reset_party(self):
        print("resetting party affiliation...", file=sys.stderr)
        self.cur.execute("""update vn_record
set party_id=null""")

    def process_page(self, card_url, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print(card_url + " not found on disk", file=sys.stderr)
            return

        try:
            self.condensate(card_url, url_id, reader)
        finally:
            reader.close()

    def process_profile(self, town_url, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print(card_url + " not found on disk", file=sys.stderr)
            return

        try:
            self.identify_profile(town_url, url_id, reader)
        finally:
            reader.close()

    def condensate(self, card_url, card_url_id, fp):
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
                    record_id = self.condensate_record(person, hamlet_name, card_url_id)
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

    def identify_profile(self, town_url, url_id, fp):
        # can't use self.town_rx - it has a different host...
        pr = urlparse(town_url)
        town_name = pr.path[1:]

        context = etree.iterparse(fp, events=('end',), tag=('a'), html=True, recover=True)
        for action, elem in context:
            if elem.tag == 'a':
                cls = elem.get('class')
                if cls and ('ProfileHeaderCard-nameLink' in cls):
                    self.identify_record(town_name, elem.text)

            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    def condensate_record(self, person, hamlet_name, card_url_id):
        self.cur.execute("""insert into vn_record(hamlet_name, presentation_name, card_url_id)
values(%s, %s, %s)
on conflict do nothing
returning id""", (hamlet_name, person.presentation_name, card_url_id))
        row = self.cur.fetchone()
        if row is not None:
            return row[0]

        self.cur.execute("""select id
from vn_record
where hamlet_name=%s""", (hamlet_name,))
        row = self.cur.fetchone()
        return row[0]

    def identify_record(self, town_name, raw_text):
        if not raw_text:
            return

        ltext = raw_text.lower()
        text = whitespace_rx.sub(" ", ltext.strip())
        mcnt = 0;
        for record_id, query_name in self.rec2qname.items():
            if query_name in text:
                print("%d <=> %s" % (record_id, town_name), file=sys.stderr)
                self.cur.execute("""insert into vn_identity_town(record_id, town_name)
values(%s, %s)
on conflict do nothing""", (record_id, town_name))
                mcnt += 1

        if mcnt > 1:
            print("%s matches %d persons" % (town_name, mcnt), file=sys.stderr)

    def condensate_party(self, record_id, person):
        if self.funnel_links >= 2:
            self.rec2qname[record_id] = person.query_name

        name_rx = re.compile("\\b" + re.escape(person.query_name) + "\\b", re.IGNORECASE)
        gov_url = make_meta_query_url()
        query_urls = [ gov_url ]
        query_urls.extend(make_personage_query_urls(person))

        wikidata_id = None
        party_spec = None
        found_gov_spec = False
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
                                party_spec = PartySpec(
                                    id_url=id_url,
                                    long_name=it['t']['value'],
                                    short_name=get_opt(it, 'z'),
                                    color=get_opt(it, 'c'),
                                    from_year=get_from_year(it))

                                if query_url == gov_url:
                                    found_gov_spec = True
                            elif party_spec.id_url != id_url:
                                if found_gov_spec:
                                    print("ignoring minister's other parties", file=sys.stderr)
                                else:
                                    print("person matches multiple parties...", file=sys.stderr)
                                    if party_spec.from_year is None:
                                        print("...with no start date", file=sys.stderr)
                                        return

                                    cur_from = get_from_year(it)
                                    if cur_from is None:
                                        print("...without start date", file=sys.stderr)
                                        return

                                    if cur_from == party_spec.from_year:
                                        print("...with the same start date", file=sys.stderr)
                                        return

                                    if cur_from > party_spec.from_year:
                                        party_spec = PartySpec(
                                            id_url=id_url,
                                            long_name=it['t']['value'],
                                            short_name=get_opt(it, 'z'),
                                            color=get_opt(it, 'c'),
                                            from_year=cur_from)

        if party_spec:
            party_id = self.insert_party(party_spec)
            self.update_record(record_id, party_id)

    def insert_identity(self, record_id, town_name):
        self.cur.execute("""insert into vn_identity_hamlet(record_id, town_name)
values(%s, %s)
on conflict do nothing""", (record_id, town_name))

    def insert_party(self, party_spec):
        self.cur.execute("""select party_id
from vn_party_name
where party_name=%s""", (party_spec.long_name,))
        row = self.cur.fetchone()
        if not row:
            self.cur.execute("""insert into vn_party(color)
values(%s)
returning id""", (party_spec.color,))
            row = self.cur.fetchone()
            party_id = row[0]
            self.cur.execute("""insert into vn_party_name(party_id, party_name)
values(%s, %s)
""", (party_id, party_spec.long_name))
        else:
            party_id = row[0]

        if party_spec.short_name:
            self.cur.execute("""insert into vn_party_name(party_id, party_name)
values(%s, %s)
on conflict do nothing
""", (party_id, party_spec.short_name))

        return party_id

    def update_record(self, record_id, party_id):
        self.cur.execute("""update vn_record
set party_id=%s
where id=%s""", (party_id, record_id))

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            condensator = Condensator(cur)
            try:
                if get_option("condensate_reset_party", ""):
                    condensator.reset_party()

                condensator.run()
            finally:
                condensator.close()


if __name__ == "__main__":
    main()
