#!/usr/bin/python3

import json
from lxml import etree
import re
import sys
from common import get_option
from personage import parse_personage
from url_heads import green_url_head, hamlet_url_head, town_url_head
from urlize import create_query_url

def make_meta_query_url():
    # not needed for anything but hostname whitelisting yet
    query = """select ?c {
  ?c wdt:P31 wd:Q146.
}
limit 2"""
    return create_query_url(query)


def make_personage_query_url(person):
    query = """select ?w ?l ?p ?t {
  ?w wdt:P27 wd:Q213;
     wdt:P106 wd:Q82955;
     rdfs:label ?l;
     p:P102 ?s;
     wdt:P569 ?b.
  ?s ps:P102 ?p.
  filter(lang(?l) = "cs" && contains(lcase(?l), "%s") && year(?b) = %d)
  minus { ?s pq:P582 ?e. }
  ?p rdfs:label ?t.
  filter(lang(?t) = "cs")
}""" % (person.query_name, person.birth_year)
    return create_query_url(query)


class FunnelParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.page_url = url

        schema = (
            ( "^" + hamlet_url_head + "\\?desc=1&page=(?P<page>\\d+)&q=server%3ATwitter&sort=datum$", self.process_overview ),
            ( "^" + green_url_head + "(?P<hname>[-a-zA-Z0-9]+)$", self.process_card )
        )

        self.match = None
        for url_rx, proc_meth in schema:
            m = re.match(url_rx, url)
            if m:
                self.match = m
                self.process = proc_meth
                break

    def parse_links(self, fp):
        if not self.match:
            # wikidata queries
            return

        self.process(fp)

    def process_overview(self, fp):
        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))

        items = doc.get('results')
        page_size = len(items)
        page = int(self.match.group('page'))
        if (page == 1) and (page_size > 0):
            total = int(doc.get('total'))
            n = total // page_size
            i = 2
            while i <= n:
                url = hamlet_url_head + ("?desc=1&page=%d&q=server%%3ATwitter&sort=datum" % i)
                self.owner.add_link(url)
                i += 1

        for et in items:
            hamlet_name = et.get('osobaid')
            card_url = green_url_head + hamlet_name
            self.owner.add_link(card_url)

    def process_card(self, fp):
        hamlet_name = self.match.group('hname')

        card_rx = re.compile("^" + town_url_head + "/(?P<tname>[^/]+)")

        # no need to handle relative URLs - we're only interested in
        # the absolute one to Twitter
        context = etree.iterparse(fp, events=('end',), tag=('a', 'title'), html=True, recover=True)
        person = None
        for action, elem in context:
            if elem.tag == 'title':
                person = parse_personage(elem.text)
                if person:
                    wikidata_url = make_personage_query_url(person)
                    self.owner.add_link(wikidata_url)
            elif elem.tag == 'a':
                href = elem.get('href')
                if href:
                    m = card_rx.match(href)
                    if m:
                        town_name = m.group('tname')
                        # social media links are in body, so person
                        # from title should be initialized by now
                        present_name = person.presentation_name if person else None
                        print("%s <=> %s (%s)" % (hamlet_name, town_name, present_name), file=sys.stderr)
                        self.owner.cur.execute("""insert into vn_identity_hamlet(hamlet_name, town_name, presentation_name)
values(%s, %s, %s)
on conflict do nothing""", (hamlet_name, town_name, present_name))

            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]


if __name__ == "__main__":
    # needed by seed.sh
    print(make_meta_query_url())
