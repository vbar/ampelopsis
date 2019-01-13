#!/usr/bin/python3

from datetime import datetime
import re
from corrector import Corrector
from json_tree_check import JsonTreeCheck
from levels import JudgeLevel, MuniLevel, ParliamentLevel
from named_entities import Entity, councillor_position_entities, deputy_mayor_position_entities, mayor_position_entities
from rulebook import Rulebook
from rulebook_util import get_org_name, school_name_rx
from urlize import create_query_url, whitespace_rx

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

city_start_rx = re.compile("^(?:mč|město|měú|městská část|městský obvod|mo|městský úřad|městys|obec|obecní úřad|oú|úřad městské části|úmč|úřad mč|úřad městského obvodu|úmo|úřad městyse|ves|statutární město|zastupitelstvo obce|zastupitelstvo města) ")

# '-' by itself would split not only Frýdek-Místek (doesn't really
# matter for SPARQL queries matching on string start) but also
# Praha-Řeporyje (more problematic); to handle abbreviations, we want
# to stop before the first '.'
city_stop_rx = re.compile("[.,;()]| - |\\bse sídlem\\b")

# Some wikidata city district labels (i.e. Řeporyje) do not contain
# the city name. 3 is a stricter limit than elsewhere, but are there
# any 2-letter district names?
city_district_rx = re.compile("^.+-([^-]{3,})$")

date_rx = re.compile("^([0-9]{4})-[0-9]{2}-[0-9]{2}")

physician_title_rx = re.compile("\\bmudr\\b")

def normalize_name(raw):
    name = name_char_rx.sub("", raw.strip())
    return name.lower()

def convert_answer_to_iterable(answer, it):
    if callable(answer): # technically we could have a cycle, but hopefully nobody will need that...
        answer = answer(it)

    if isinstance(answer, str):
        return (answer,)
    else: # must be iterable
        return answer

def convert_city_set_to_dict(city_set):
    city_dict = {}
    for city in city_set:
        m = city_district_rx.match(city)
        if m:
            local_name = m.group(1)
            match_fnc = 'contains'
        else:
            local_name = city
            # equality actually doesn't match some labels, even before we split them on '.'
            match_fnc = 'strstarts'

        # in case of key clash (which normally shouldn't happen, but
        # just to handle every case), prefer the least-strict matching
        # function
        if (match_fnc == 'strstarts') and (city_dict.get(local_name) == 'contains'):
            match_fnc = 'contains'

        city_dict[local_name] = match_fnc

    return city_dict

def format_position_iterable(position_iterable):
    return ' '.join('wd:' + p for p in sorted(position_iterable))

def format_court_set(court_set):
    terms = [ 'contains(lcase(?g), "%s")' % stem for stem in sorted(court_set) ]
    cond = ' || '.join(terms)
    return cond if len(terms) < 2 else '(%s)' % cond

def format_neg_court_set(neg_court_set):
    terms = [ '!contains(lcase(?g), "%s")' % stem for stem in sorted(neg_court_set) ]
    return ' && '.join(terms)

def format_city_set(city_set):
    city_dict = convert_city_set_to_dict(city_set)

    terms = []
    for local_name in sorted(city_dict.keys()):
        match_fnc = city_dict[local_name]
        terms.append('%s(lcase(?t), "%s")' % (match_fnc, local_name))

    cond = ' || '.join(terms)
    return cond if len(terms) < 2 else '(%s)' % cond

def format_mayor_bare_clause(mayor_position_set, city_set):
    vl = format_position_iterable(mayor_position_set)
    filter_expr = format_city_set(city_set)

    # there are 2 (known) ways to get from mayor to their municipality; we or them
    return """values ?p { %s }
        {
            ?m p:P6/ps:P6 ?w.
        } union {
            ?w p:P39/pq:P642 ?m.
        }
        ?m rdfs:label ?t.
        filter(lang(?t) = "cs" && %s)""" % (vl, filter_expr)

def format_councillor_bare_clause(councillor_position_iterable, city_set):
    vl = format_position_iterable(councillor_position_iterable)
    filter_expr = format_city_set(city_set)

    return """values ?p { %s }
        ?w p:P39/pq:P642/rdfs:label ?t.
        filter(lang(?t) = "cs" && %s)""" % (vl, filter_expr)

def make_mayor_of_query_url():
    vl = format_position_iterable(mayor_position_entities)
    query = """select ?q ?j ?l ?p {
        ?q wdt:P279 ?p;
                wdt:P1001 ?j.
        values ?p { %s }
        ?j wdt:P17 wd:Q213;
                rdfs:label ?l.
        filter(lang(?l) = "cs")
}""" % vl
    return create_query_url(query)

def wrap_pos_clause(bare_clause, occ_exists):
    pos_branch = []
    if occ_exists:
        pos_branch.append('?w wdt:P39 ?p.')

    pos_branch.append(bare_clause)
    return ''.join(pos_branch)

class Jumper:
    def __init__(self):
        # core analysis
        self.rulebook = Rulebook()

        today = datetime.now()
        self.last_year = today.year - 2

        self.physician_check = JsonTreeCheck('titleBefore', physician_title_rx)

        self.city2mayor = {}

        # hardcoded, for now - Wikidata probably doesn't have a a city
        # assembly member entity for any Czech city beside Prague...
        self.city2councillor = {
            'praha': 'Q27830380',
        }

        # hardcoded list is not really satisfactory...
        self.name2city = {
            'magistrát města české budějovice': 'české budějovice',
            'magistrát města chomutova': 'chomutov',
            'magistrát města ústí n.l.': 'ústí nad labem',
            'magistrát města mladá boleslav': 'mladá boleslav',
            'magistrát mladá boleslav': 'mladá boleslav',
            'magistrát města mostu': 'most',
            'magistrát města opavy': 'opava',
            'magistrát města pardubic': 'pardubice',
            'magistrát města plzně': 'plzeň',
            'magistrát hlavního města prahy': 'praha',
            'úřad městské části města brna, brno-komín': 'brno',
        }

        # Does not include district & regional courts - for those we
        # want a negative match. Values must be substrings of
        # genitive.
        self.court2stem = {
            'nejvyšší soud': 'nejvyššího soudu', # do not match nejvyšší správní soud - that's considered non-prominent
            'ústavní soud': 'ústavní', # ústavního
            'vrchní soud v praze': 'vrchní', # vrchního
            'vrchní soud v olomouci': 'vrchní',
        }

        self.neg_court_cond = format_neg_court_set(set(self.court2stem.values()))

        self.city_office_corrector = Corrector(4, self.name2city.keys())
        self.top_prosecutors_office_corrector = Corrector(2, ('nejvyšší státní zastupitelství',))
        self.court_corrector = Corrector(3, self.court2stem.keys())

    def load(self, cur):
        cur.execute("""select municipality, wd_entity
from cro_mayor_of
order by wd_entity""")
        rows = cur.fetchall()
        for row in rows:
            self.city2mayor[row[0]] = row[1]

    def store(self, cur):
        for city, mayor in self.city2mayor.items():
            cur.execute("""insert into cro_mayor_of(wd_entity, municipality)
values(%s, %s)
on conflict(wd_entity) do update
set municipality=%s""", (mayor, city, city))

    def add_muni_mayor(self, city, mayor):
        norm_muni = self.normalize_city(city)
        if len(norm_muni) > 1:
            self.city2mayor[norm_muni] = mayor

    def normalize_city(self, raw):
        name = raw.lower()

        city_offices = self.city_office_corrector.match(name)
        l = len(city_offices)
        if l > 1:
            raise Exception("internal error: name2city has approximately duplicate keys")

        if l == 1:
            return self.name2city[city_offices.pop()]

        lst = city_stop_rx.split(name, maxsplit=1)
        head = lst[0]
        safer = name_char_rx.sub("", head.strip()) # maybe we need a more permissive regex, but nothing specific comes to mind...
        shorter = city_start_rx.sub("", safer)
        return whitespace_rx.sub(" ", shorter.strip())

    def make_person_name(self, detail):
        return "%s %s" % tuple(normalize_name(detail[n]) for n in ('firstName', 'lastName'))

    def make_position_set(self, detail):
        sought = set()

        # will probably (but not provably) also be handled by
        # rulebook; Entity.judge should work outside rulebook (unlike
        # e.g. Entity.mp) because judges w/o JudgeLevel matches are
        # considered non-prominent and get a negative court set
        if detail['judge']:
            sought.add(Entity.judge)

        if self.physician_check.walk(detail):
            sought.add(Entity.physician)

        lst = detail['workingPositions']
        for it in lst:
            if len(self.top_prosecutors_office_corrector.match(get_org_name(it))):
                sought.add('Q26197430') # now redirects to
                sought.add('Q12040609')

            wp = it['workingPosition']
            answer = self.rulebook.get(wp['name'])
            if answer:
                answer = convert_answer_to_iterable(answer, it)
                sought.update(answer)

            # checking wp['deputy'] and wp['senator'] here is
            # possible, but it makes no difference (all MPs have
            # wp['name'] 'poslanec', and senators 'senátor'), and
            # adding MP terms wouldn't work outside rulebook anyway...

        if self.check_school(detail):
            sought.add(Entity.pedagogue)
            # could also add researcher here, but it doesn't match
            # anybody new...

        return sought

    def check_school(self, detail):
        for statement in detail['statements']:
            incomes = statement.get('incomes')
            if incomes:
                for income in incomes:
                    legal = income.get('legalPerson')
                    if legal:
                        nm = legal.get('name')
                        if nm:
                            org_name = nm.lower()
                            if school_name_rx.search(org_name):
                                return True

        return False

    def make_court_set(self, detail):
        sought = set()
        lst = detail['workingPositions']
        for it in lst:
            wp = it['workingPosition']
            answer = self.rulebook.get(wp['name'])
            if answer is not None and isinstance(answer, JudgeLevel):
                courts = self.court_corrector.match(get_org_name(it))
                for court in courts:
                    sought.add(self.court2stem[court])

        return sought

    def make_city_set(self, detail, representative):
        sought = set()
        lst = detail['workingPositions']
        for it in lst:
            wp = it['workingPosition']
            answer = self.rulebook.get(wp['name'])
            if answer is not None and isinstance(answer, MuniLevel):
                answer = convert_answer_to_iterable(answer, it)
                if representative in answer:
                    norm_muni = self.normalize_city(it['organization'])
                    if len(norm_muni) > 1: # Aš
                        sought.add(norm_muni)

        return sought

    def fold_min_start(self, detail, representative):
        year = None
        lst = detail['workingPositions']
        for it in lst:
            wp = it['workingPosition']
            answer = self.rulebook.get(wp['name'])
            if answer is not None and isinstance(answer, ParliamentLevel):
                answer = convert_answer_to_iterable(answer, it)
                if representative in answer:
                    for start_name in ('start', 'dateOfStart'):
                        start = it.get(start_name)
                        if start:
                            m = date_rx.match(start)
                            if m:
                                y = int(m.group(1))
                                if (year is None) or (year > y):
                                    year = y

        return year

    def make_query_url(self, detail, position_set):
        name_cond = 'contains(lcase(?l), "%s")' % self.make_person_name(detail)

        specific = len(position_set)
        position_list = list(position_set)

        minister_position = None
        if Entity.minister in position_set:
            position_set.remove(Entity.minister)
            minister_position = Entity.minister

        mp_position = None
        if Entity.mp in position_set:
            # not removed from position_set
            mp_position = Entity.mp

        judge_position = None
        court_set = None
        if Entity.judge in position_set:
            position_set.remove(Entity.judge)
            judge_position = Entity.judge
            court_set = self.make_court_set(detail)

        prosecutor_position = None
        if Entity.prosecutor in position_set:
            position_set.remove(Entity.prosecutor)
            prosecutor_position = Entity.prosecutor

        mayor_position_set = set()
        for pos in mayor_position_entities:
            if pos in position_list:
                position_set.remove(pos)
                mayor_position_set.add(pos)

        deputy_mayor_position_set = set()
        for pos in deputy_mayor_position_entities:
            if pos in position_list:
                position_set.remove(pos)
                deputy_mayor_position_set.add(pos)

        councillor_position_set = set()
        for pos in councillor_position_entities:
            if pos in position_list:
                position_set.remove(pos)
                councillor_position_set.add(pos)

        occupation_list = []
        # judge is not included because it's required when present in input
        if prosecutor_position:
            occupation_list.append(prosecutor_position)

        for occupation in (Entity.police_officer, Entity.physician, Entity.psychiatrist, Entity.researcher, Entity.pedagogue):
            if occupation in position_set:
                position_set.remove(occupation)
                occupation_list.append(occupation)

        l0 = len(occupation_list)

        pos_clauses = []
        if minister_position:
            np = 'wd:' + minister_position
            pos_clauses.append(wrap_pos_clause('?p wdt:P279/wdt:P279 %s.' % np, l0))

        min_year = None
        if mp_position:
            min_year = self.fold_min_start(detail, mp_position)
            # should have filtered on year here but that times out
            # when nested in a union, so we handle it not by a
            # position clause but by a filter on top level

        if len(mayor_position_set):
            city_set = self.make_city_set(detail, mayor_position_entities[0])
            for city in city_set:
                mayor = self.city2mayor.get(city)
                if mayor:
                    position_set.add(mayor)

            if len(city_set):
                bare_clause = format_mayor_bare_clause(mayor_position_set, city_set)
                pos_clauses.append(wrap_pos_clause(bare_clause, l0))

        deputy_mayor_city_set = set()
        if len(deputy_mayor_position_set):
            # Constructing city set separately for deputy mayor
            # actually leads to fewer matches - maybe it's just a time
            # mismatch, and we should do it (like for bank governor)?
            # OTOH there's more municipal councillors than central
            # bank's - let's distinguish, for now...
            deputy_mayor_city_set = self.make_city_set(detail, deputy_mayor_position_entities[0])

        councillor_city_set = set()
        if len(councillor_position_set):
            councillor_city_set = self.make_city_set(detail, councillor_position_entities[0])
            for city in councillor_city_set:
                for short_muni, councillor in self.city2councillor.items():
                    if re.match("^" + re.escape(short_muni) + "\\b", city): # e.g. praha 3
                        position_set.add(councillor)

        # deputy mayor and councillor have the same bare clause; if their
        # city set is the same, they can be combined
        if len(deputy_mayor_city_set) and (deputy_mayor_city_set == councillor_city_set):
            assert len(deputy_mayor_position_set)
            assert len(councillor_city_set)
            assert len(councillor_position_set)
            councillor_city_set |= deputy_mayor_city_set
            deputy_mayor_city_set = set()
            councillor_position_set |= deputy_mayor_position_set
            deputy_mayor_position_set = set()

        if len(deputy_mayor_city_set):
            assert len(deputy_mayor_position_set)
            bare_clause = format_councillor_bare_clause(deputy_mayor_position_set, deputy_mayor_city_set)
            pos_clauses.append(wrap_pos_clause(bare_clause, l0))

        if len(councillor_city_set):
            assert len(councillor_position_set)
            bare_clause = format_councillor_bare_clause(councillor_position_set, councillor_city_set)
            pos_clauses.append(wrap_pos_clause(bare_clause, l0))

        if len(position_set):
            vl = format_position_iterable(position_set)
            pos_clauses.append(wrap_pos_clause('values ?p { %s }' % vl, l0))

        l = len(pos_clauses)

        # judge is such a specific feature we require it when present
        # in input data (rather than or-ing it with political
        # positions); that (plus multiple refactorings) complicates
        # the policeman & doctor case...
        political_constraint = ''
        mainline_block = ''
        loc_occ = False
        if judge_position:
            if not l:
                political_constraint = 'wdt:P106 ?o;'
                mainline_block = 'optional { ?w wdt:P39 ?p. }'
            else:
                political_constraint = 'wdt:P39 ?p; wdt:P106 ?o;'

            np = 'wd:' + judge_position
            mainline_block += 'values ?o { %s }' % np
        else:
            if not l0:
                if specific:
                    political_constraint ='wdt:P39 ?p;'
                    mainline_block = 'optional { ?w wdt:P106 ?o. }'
                else:
                    mainline_block = """optional { ?w wdt:P39 ?p. }
        optional { ?w wdt:P106 ?o. }"""
            else:
                if not l:
                    if not prosecutor_position:
                        political_constraint = 'wdt:P106 ?o;'
                        mainline_block = 'optional { ?w wdt:P39 ?p. }'
                    else:
                        loc_occ = True
                else:
                    loc_occ = True

        if l0:
            occ_branch = []
            if loc_occ:
                occ_branch.append('?w wdt:P106 ?o.')

            vl = format_position_iterable(occupation_list)
            occ_branch.append('values ?o { %s }' % vl)

            pos_clauses.append(''.join(occ_branch))

            # prosecutor already is in occupation_list (and therefore
            # in pos_clauses), but the occupation almost never matches
            # - so we also try to match description...
            if prosecutor_position:
                pos_clauses.append('filter(contains(lcase(?g), "státní zástup"))') # zástupce, zástupkyně

        l = len(pos_clauses)
        if l == 0:
            # no restriction; can happen even when the original
            # position set is non-empty - if that causes false
            # positives, we'll have to revisit...
            pos_clause = ''
        elif l == 1:
            pos_clause = pos_clauses[0]
        else:
            pos_clause = ' union '.join('{ %s }' % pc for pc in pos_clauses)

        extra_block = ''
        if min_year:
            assert mp_position
            assert l

            # ?t is already taken; 'coalesce(?f, ?u) >=
            # "%d-01-01"^^xsd:dateTime' might be more efficient...
            base_cond = 'year(coalesce(?f, ?u)) >= %d' % min_year
            # mp_position is in position set...
            if (l == 1) and (len(position_set) == 1):
                # ...so position set == mp_position
                cond = base_cond
            else:
                np = 'wd:' + mp_position
                cond = '?p != %s || %s' % (np, base_cond)

            extra_block = """optional { ?w p:P39/pq:P580 ?f. }
        optional { ?w p:P39/pq:P582 ?u. }
        filter(%s)""" % cond

        death_clause = ''
        if specific:
             death_clause = """optional { ?w wdt:P570 ?d. }
        filter(!bound(?d) || year(?d) >= %d)""" % self.last_year

        judge_cond = ''
        if judge_position:
            if len(court_set):
                base_cond = format_court_set(court_set)
            else:
                # non-prominent judge
                base_cond = self.neg_court_cond

            # preceded by name_cond; unbound ?g matches iff the judge
            # is non-prominent
            judge_cond = ' && ' + base_cond

        # person (wikidata ID), article, birth, label, general description, position, occupation
        query = """select ?w ?a ?b ?l ?g ?p ?o {
        ?w wdt:P27 wd:Q213;
                rdfs:label ?l;
                %s
                wdt:P569 ?b.
        %s%s
        optional {
                ?a schema:about ?w;
                        schema:inLanguage "cs".
        }
        optional {
                ?w schema:description ?g.
                filter(lang(?g) = "cs")
        }
        filter(lang(?l) = "cs" && %s%s)
        %s %s
}""" % (political_constraint, death_clause, mainline_block, name_cond, judge_cond, pos_clause, extra_block)
        return create_query_url(query)

if __name__ == "__main__":
    # needed by seed.sh
    print(make_mayor_of_query_url())
