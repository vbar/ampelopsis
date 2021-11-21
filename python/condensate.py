#!/usr/bin/python3

import dateparser
from lxml import etree
import re
import sys
from baker import WOOD
from common import make_connection
from html_lookup import make_card_query_urls
from json_frame import JsonFrame
from url_templates import speaker_minister_tmpl, speaker_mp_tmpl

wikidata_rx = re.compile('^http://www.wikidata.org/entity/(Q[0-9]+)$')

class Condensator(JsonFrame):
    def __init__(self, cur):
        JsonFrame.__init__(self, cur)

    def run(self):
        speaker_pattern = "^(%s|%s)" % (re.escape(speaker_mp_tmpl), re.escape(speaker_minister_tmpl))
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '%s'
and checkd is not null
and url_id is null
order by url""" % speaker_pattern)
        rows = self.cur.fetchall()
        for row in rows:
            self.process_card(*row)

    def process_card(self, card_url, url_id):
        print("checking %s..." % (card_url,), file=sys.stderr)

        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            raise Exception(card_url + " not downloaded")

        try:
            qurls = make_card_query_urls(card_url, WOOD, reader)
            for qurl in qurls:
                self.process_query(url_id, qurl)
        finally:
            reader.close()

    def process_query(self, card_url_id, url):
        url_id = self.get_url_id(url)
        if not url_id:
            return

        doc = self.get_document(url_id)
        if not doc or not isinstance(doc, dict):
            return

        results = doc.get('results')
        if not results or not isinstance(results, dict):
            return

        bindings = results.get('bindings')
        if not bindings or not isinstance(bindings, list):
            return

        for answer in bindings:
            if not isinstance(answer, dict):
                raise Exception("unexpected answer format")

            party_id = self.process_party(answer)
            person_id = self.process_person(card_url_id, answer)
            if person_id and party_id:
                self.process_membership(person_id, party_id, answer)

    def process_party(self, answer):
        party = self.get_wikidata_id(answer.get('p'))
        if not party:
            return None

        color = self.get_literal(answer.get('c'))

        party_id = None
        self.cur.execute("""insert into ast_party(wikidata_id, color)
values(%s, %s)
on conflict do nothing
returning id""", (party, color))
        row = self.cur.fetchone()
        if row:
            party_id = row[0]

        if party_id is None:
            self.cur.execute("""select id
from ast_party
where wikidata_id=%s""", (party,))
            row = self.cur.fetchone()
            party_id = row[0]
            if color:
                self.cur.execute("""update ast_party
set color=%s
where id=%s""", (color, party_id))

        for var_name in ('t', 'z'):
            party_name = self.get_literal(answer.get(var_name))
            if party_name:
                self.cur.execute("""insert into ast_party_name(party_id, party_name)
values(%s, %s)
on conflict do nothing""", (party_id, party_name))

        return party_id

    def process_person(self, card_url_id, answer):
        person = self.get_wikidata_id(answer.get('w'))
        if not person:
            return None

        person_name = self.get_literal(answer.get('l'))

        person_id = None
        self.cur.execute("""insert into ast_person(wikidata_id, presentation_name)
values(%s, %s)
on conflict do nothing
returning id""", (person, person_name))
        row = self.cur.fetchone()
        if row:
            person_id = row[0]

        if person_id is None:
            self.cur.execute("""select id from ast_person
where wikidata_id=%s""", (person,))
            row = self.cur.fetchone()
            person_id = row[0]
            if person_name:
                # keeping the last name - this could be problematic
                # for people changing their names, but can wait until
                # we actually see it...
                self.cur.execute("""update ast_person
set presentation_name=%s
where id=%s""", (person_name, person_id))

        self.cur.execute("""insert into ast_identity_card(person_id, link_id)
values(%s, %s)
on conflict do nothing""", (person_id, card_url_id))

        return person_id

    def process_membership(self, person_id, party_id, answer):
        from_date = self.get_date_literal(answer.get('f'))
        until_date = self.get_date_literal(answer.get('u'))
        if from_date:
            if until_date:
                self.insert_bounded(person_id, party_id, from_date, until_date)
            else:
                self.insert_starting(person_id, party_id, from_date)
        else:
            if until_date:
                self.insert_ending(person_id, party_id, until_date)
            else:
                self.insert_unbounded(person_id, party_id)

    def insert_unbounded(self, person_id, party_id):
        self.cur.execute("""select count(*)
from ast_party_member
where person_id=%s and party_id=%s and from_date is null and until_date is null""", (person_id, party_id))
        row = self.cur.fetchone()
        if not row[0]:
            self.cur.execute("""insert into ast_party_member(person_id, party_id)
values(%s, %s)""", (person_id, party_id))
        # else the record is already there

    def insert_starting(self, person_id, party_id, from_date):
        self.cur.execute("""select from_date
from ast_party_member
where person_id=%s and party_id=%s and from_date is not null and until_date is null""", (person_id, party_id))
        rows = self.cur.fetchall()
        for row in rows:
            old_from = row[0]
            if old_from > from_date:
                self.cur.execute("""update ast_party_member
set from_date=%s
where person_id=%s and party_id=%s and from_date is not null and until_date is null""", (from_date, person_id, party_id))

            return

        self.cur.execute("""insert into ast_party_member(person_id, party_id, from_date)
values(%s, %s, %s)""", (person_id, party_id, from_date))

    def insert_ending(self, person_id, party_id, until_date):
        self.cur.execute("""select until_date
from ast_party_member
where person_id=%s and party_id=%s and from_date is null and until_date is not null""", (person_id, party_id))
        rows = self.cur.fetchall()
        for row in rows:
            old_until = row[0]
            if old_until < until_date:
                self.cur.execute("""update ast_party_member
set until_date=%s
where person_id=%s and party_id=%s and from_date is null and until_date is not null""", (until_date, person_id, party_id))

            return

        self.cur.execute("""insert into ast_party_member(person_id, party_id, until_date)
values(%s, %s, %s)""", (person_id, party_id, until_date))

    def insert_bounded(self, person_id, party_id, from_date, until_date):
        assert from_date
        assert until_date

        self.cur.execute("""select from_date, until_date
from ast_party_member
where person_id=%s and party_id=%s and from_date is not null and until_date is not null
order by from_date, until_date""", (person_id, party_id))
        rows = self.cur.fetchall()
        ext_count = 0
        for sweep_from, sweep_until in rows:
            if sweep_from > sweep_until:
                raise Exception("invalid interval")

            if (from_date <= sweep_from) and (until_date >= sweep_from):
                if (ext_count > 1) and ((from_date != sweep_from) or (until_date != sweep_until)):
                    self.cur.execute("""delete
from ast_party_member
where person_id=%s and party_id=%s and from_date=%s and until_date=%s""", (person_id, party_id, from_date, until_date))

                if until_date < sweep_until:
                    until_date = sweep_until

                if (from_date != sweep_from) or (until_date != sweep_until):
                    self.cur.execute("""update ast_party_member
set from_date=%s, until_date=%s
where person_id=%s and party_id=%s and from_date=%s and until_date=%s""", (from_date, until_date, person_id, party_id, sweep_from, sweep_until))

                ext_count += 1

        if not ext_count:
            self.cur.execute("""insert into ast_party_member(person_id, party_id, from_date, until_date)
values(%s, %s, %s, %s)""", (person_id, party_id, from_date, until_date))

    @staticmethod
    def get_wikidata_id(d):
        if not d:
            return None

        if not isinstance(d, dict):
            raise Exception("unexpected SPARQL format")

        if d.get('type') != 'uri':
            raise Exception("unexpected URI variable format")

        v = d.get('value')
        if not v:
            raise Exception("empty variable")

        m = wikidata_rx.match(v)
        if not m:
            raise Exception("unexpected value " + v)

        return m.group(1)

    @staticmethod
    def get_literal(d):
        if not d:
            return None

        if not isinstance(d, dict):
            raise Exception("unexpected SPARQL format")

        tp = d.get('type')
        if tp != 'literal':
            raise Exception("unexpected literal variable format: " + tp)

        return d.get('value')

    @staticmethod
    def get_date_literal(d):
        if not d:
            return None

        if not isinstance(d, dict):
            raise Exception("unexpected SPARQL format")

        tp = d.get('type')
        if tp == 'uri': # apparently used for values that went away...
            return None

        if tp != 'literal':
            raise Exception("unexpected literal variable format: " + tp)

        datatype = d.get('datatype')
        if datatype != 'http://www.w3.org/2001/XMLSchema#dateTime':
            raise Exception("unexpected date type " + datatype)

        v = d.get('value')
        if not v:
            return None

        dt = dateparser.parse(v)
        if not dt:
            return None

        # actually can have even lower precision, but we're ignoring
        # that (so far)...
        return dt.date()


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            condensator = Condensator(cur)
            try:
                condensator.run()
            finally:
                condensator.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
