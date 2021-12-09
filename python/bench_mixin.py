import dateparser
import re
import sys

wikidata_rx = re.compile('^http://www.wikidata.org/entity/(Q[0-9]+)$')

class BenchMixin: # expects JsonFrame as another inherited class
    def process_gov(self, url):
        url_id = self.get_url_id(url)
        if not url_id:
            return

        bindings = self.get_bindings(url_id)
        if bindings is None:
            return

        for answer in bindings:
            if not isinstance(answer, dict):
                raise Exception("unexpected answer format")

            party_id = self.process_party(answer)
            person_id = self.process_minister(answer)
            if person_id and party_id:
                # we could use the government life span, but a)
                # minister's tenure might be shorter, and b) just
                # because a government had ended doesn't mean its
                # ministers changed party affiliation...
                self.insert_unbounded(person_id, party_id)

    def process_query(self, card_url_id, birth_year, position_set, url):
        url_id = self.get_url_id(url)
        if not url_id:
            return

        bindings = self.get_bindings(url_id)
        if bindings is None:
            return

        for answer in bindings:
            if not isinstance(answer, dict):
                raise Exception("unexpected answer format")

            party_id = self.process_party(answer)
            person_id = self.process_person(card_url_id, birth_year, position_set, answer)
            if person_id and party_id:
                self.process_membership(person_id, party_id, answer)

    def get_bindings(self, url_id):
        doc = self.get_document(url_id)
        if not doc or not isinstance(doc, dict):
            return None

        results = doc.get('results')
        if not results or not isinstance(results, dict):
            return None

        bindings = results.get('bindings')
        if not bindings or not isinstance(bindings, list):
            return None

        return bindings

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

    def process_person(self, card_url_id, birth_year, position_set, answer):
        person = self.get_wikidata_id(answer.get('w'))
        if not person:
            return None

        person_name = self.get_literal(answer.get('l'))
        position_list = " ".join(sorted(position_set)) if position_set else None

        person_id = None
        self.cur.execute("""insert into ast_person(presentation_name, birth_year, position_list, wikidata_id)
values(%s, %s, %s, %s)
on conflict do nothing
returning id""", (person_name, birth_year, position_list, person))
        row = self.cur.fetchone()
        if row:
            person_id = row[0]

        if person_id is None:
            self.cur.execute("""select id, birth_year, position_list from ast_person
where wikidata_id=%s""", (person,))
            row = self.cur.fetchone()
            person_id = row[0]
            old_birth_year = row[1]
            old_position_list = row[2]
            if person_name:
                # keeping the last name - this could be problematic
                # for people changing their names, but can wait until
                # we actually see it...
                self.cur.execute("""update ast_person
set presentation_name=%s
where id=%s""", (person_name, person_id))

            if birth_year:
                if old_birth_year:
                    if birth_year != old_birth_year:
                        raise Exception("%s changed birth year" % person_name)
                    # else no change is needed
                else:
                    self.cur.execute("""update ast_person
set birth_year=%s
where id=%s""", (birth_year, person_id))

            if position_list:
                if old_position_list:
                    if position_list != old_position_list:
                        # this could happen, but not until we get to
                        # tracking more than 1 position...
                        raise Exception("%s changed position" % person_name)
                    # else no change is needed
                else:
                    self.cur.execute("""update ast_person
set position_list=%s
where id=%s""", (position_list, person_id))

        if card_url_id:
            self.cur.execute("""insert into ast_identity_card(person_id, link_id)
values(%s, %s)
on conflict do nothing""", (person_id, card_url_id))

        return person_id

    def process_minister(self, answer):
        person = self.get_wikidata_id(answer.get('w'))
        if not person:
            return None

        person_name = self.get_literal(answer.get('l'))

        person_id = None
        self.cur.execute("""insert into ast_person(presentation_name, wikidata_id)
values(%s, %s)
on conflict do nothing
returning id""", (person_name, person))
        row = self.cur.fetchone()
        if row:
            person_id = row[0]

        if person_id is None:
            self.cur.execute("""select id from ast_person
where wikidata_id=%s""", (person,))
            row = self.cur.fetchone()
            person_id = row[0]

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
        self.cur.execute("""select id, from_date
from ast_party_member
where person_id=%s and party_id=%s and from_date is not null and until_date is null""", (person_id, party_id))
        rows = self.cur.fetchall()
        for interval_id, old_from in rows:
            if old_from > from_date:
                self.cur.execute("""update ast_party_member
set from_date=%s
where id=%s""", (from_date, interval_id))

            return

        self.cur.execute("""insert into ast_party_member(person_id, party_id, from_date)
values(%s, %s, %s)""", (person_id, party_id, from_date))

    def insert_ending(self, person_id, party_id, until_date):
        self.cur.execute("""select id, until_date
from ast_party_member
where person_id=%s and party_id=%s and from_date is null and until_date is not null""", (person_id, party_id))
        rows = self.cur.fetchall()
        for interval_id, old_until in rows:
            if old_until < until_date:
                self.cur.execute("""update ast_party_member
set until_date=%s
where id=%s""", (until_date, interval_id))

            return

        self.cur.execute("""insert into ast_party_member(person_id, party_id, until_date)
values(%s, %s, %s)""", (person_id, party_id, until_date))

    def insert_bounded(self, person_id, party_id, from_date, until_date):
        assert from_date
        assert until_date

        if from_date > until_date:
            print("ignoring statement with switched bounds", file=sys.stderr)
            return

        self.cur.execute("""select id, from_date, until_date
from ast_party_member
where person_id=%s and party_id=%s and from_date is not null and until_date is not null
order by from_date, until_date""", (person_id, party_id))
        rows = self.cur.fetchall()
        last_id = None
        for interval_id, sweep_from, sweep_until in rows:
            if sweep_from > sweep_until:
                raise Exception("invalid interval")

            if (from_date <= sweep_from) and (until_date >= sweep_from):
                if (last_id is not None) and ((from_date != sweep_from) or (until_date != sweep_until)):
                    self.cur.execute("""delete
from ast_party_member
where id=%s""", (last_id,))

                if until_date < sweep_until:
                    until_date = sweep_until

                if (from_date != sweep_from) or (until_date != sweep_until):
                    self.cur.execute("""update ast_party_member
set from_date=%s, until_date=%s
where id=%s""", (from_date, until_date, interval_id))

                last_id = interval_id

        if last_id is None:
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
