import re
from interval_inserter import IntervalInserter

wikidata_rx = re.compile('^http://www.wikidata.org/entity/(Q[0-9]+)$')

class BenchMixin: # expects JsonFrame as another inherited class
    def __init__(self):
        self.membership_inserter = IntervalInserter(self.cur, "ast_party_member", "party_id", "f", "u")
        self.position_inserter = IntervalInserter(self.cur, "ast_person_position", "wikidata_id", "d", "e")

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
                self.membership_inserter.insert_unbounded(person_id, party_id)

    def process_query(self, birth_year, url):
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
            person_id = self.process_person(birth_year, answer)
            if person_id:
                if party_id:
                    self.membership_inserter.process(person_id, party_id, answer)

                position = self.get_wikidata_id(answer.get('o'))
                if position:
                    self.position_inserter.process(person_id, position, answer)

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

    def process_person(self, birth_year, answer):
        person = self.get_wikidata_id(answer.get('w'))
        if not person:
            return None

        person_name = self.get_literal(answer.get('l'))

        person_id = None
        self.cur.execute("""insert into ast_person(presentation_name, birth_year, wikidata_id)
values(%s, %s, %s)
on conflict do nothing
returning id""", (person_name, birth_year, person))
        row = self.cur.fetchone()
        if row:
            person_id = row[0]

        if person_id is None:
            self.cur.execute("""select id, birth_year from ast_person
where wikidata_id=%s""", (person,))
            row = self.cur.fetchone()
            person_id = row[0]
            old_birth_year = row[1]
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
                        raise Exception("%s changed birth year from %s to %s" % (person_name, old_birth_year, birth_year))
                    # else no change is needed
                else:
                    self.cur.execute("""update ast_person
set birth_year=%s
where id=%s""", (birth_year, person_id))

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
