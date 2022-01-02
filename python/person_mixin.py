from dateutil.parser import parse
import sys
from cook import make_speaker_position_set
from html_lookup import make_card_person

class PersonMixin: # expects JsonFrame as another inherited class
    def __init__(self):
        self.direct_map = {} # ( str presentation name, int birth year ) -> int person ID
        self.cur.execute("""select id, presentation_name, birth_year
from ast_person
where birth_year is not null
order by id""")
        rows = self.cur.fetchall()
        duplicates = set()
        for person_id, presentation_name, birth_year in rows:
            k = (presentation_name, birth_year)
            if k in self.direct_map:
                duplicates.add(k)
            else:
                self.direct_map[k] = person_id

        for k in duplicates:
            print("repeated %s (%d) - ignoring..." % k, file=sys.stderr)
            del self.direct_map[k]

    def get_person(self, doc):
        card_url = doc.get('speaker_url')
        if card_url:
            person_id = self.get_person_by_card(card_url)
            if person_id:
                return person_id

        speaker_position = doc.get('speaker_position')
        speaker_name = doc.get('speaker_name')
        raw_day = doc.get('date')
        if not (speaker_position and speaker_name and raw_day):
            return None

        position_set = make_speaker_position_set(speaker_position)
        if not len(position_set):
            return None

        dt = parse(raw_day)
        day = dt.date()
        return self.get_person_by_position(speaker_name, position_set, day)

    def get_person_by_card(self, card_url):
        card_url_id = self.get_url_id(card_url)
        if not card_url_id:
            return None

        url_id = self.get_redirect_target(card_url_id)
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        try:
            person = make_card_person(card_url, reader)
            if person and person.birth_year:
                return self.get_person_by_year(person.presentation_name, person.birth_year)
        finally:
            reader.close()

        return None

    def get_person_by_year(self, presentation_name, birth_year):
        assert presentation_name and birth_year
        k = (presentation_name, birth_year)
        return self.direct_map.get(k)

    def get_person_by_position(self, presentation_name, position_set, day):
        assert presentation_name and day

        l = len(position_set)
        assert l
        if l == 1:
            pos = next(iter(position_set))
            pos_cond_tail = "='%s'" % pos
        else:
            positions = ( "'%s'" % p for p in sorted(position_set) )
            position_str = ", ".join(positions)
            pos_cond_tail = " in (%s)" % position_str
        query = """select person_id
from ast_person
join ast_person_position on ast_person.id=person_id
where (presentation_name=%s)
        and (ast_person_position.wikidata_id{tail})
        and ((from_date is null) or (from_date<=%s))
        and ((until_date is null) or (until_date>=%s))""".format(tail=pos_cond_tail)
        self.cur.execute(query, (presentation_name, day, day))
        rows = self.cur.fetchall()
        person_id = None
        for row in rows:
            if person_id is None:
                person_id = row[0]
            elif person_id != row[0]:
                raise Exception("duplicate %s %s at %s" % (position_set, presentation_name, day))

        return person_id
