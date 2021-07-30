#!/usr/bin/python3

from datetime import datetime
import re
from corrector import Corrector
from name_check import name_char_rx, normalize_name, NameCheck
from tree_check import TreeCheck
from levels import JudgeLevel, MuniLevel, ParliamentLevel
from named_entities import Entity, councillor_position_entities, deputy_mayor_position_entities, mayor_position_entities
from rulebook import Rulebook
from rulebook_util import convert_answer_to_iterable, get_org_name, reduce_substrings_to_shortest, school_name_rx
from urlize import create_query_url, whitespace_rx

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

school_tail_rx = re.compile(" [asv]$")

date_rx = re.compile("^([0-9]{4})-[0-9]{2}-[0-9]{2}")

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

def check_school(raw_name):
    if not raw_name:
        return None

    org_name = normalize_name(raw_name)
    m = school_name_rx.search(org_name)
    if not m:
        return None

    return school_tail_rx.sub("", m.group(1))

def format_position_iterable(position_iterable):
    return ' '.join('wd:' + p for p in sorted(position_iterable))

def format_string_set(sub_expr, string_set):
    assert len(string_set)
    terms = [ 'contains(lcase(%s), "%s")' % (sub_expr, stem) for stem in sorted(string_set) ]
    cond = ' || '.join(terms)
    return cond if len(terms) < 2 else '(%s)' % cond

def format_name_set(name_set):
    return format_string_set('?l', name_set)

def format_court_set(court_set):
    return format_string_set('?g', court_set)

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

def format_school_set(school_set):
    return format_string_set('?k', school_set)

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

def make_meta_query_url():
    mayor_superclasses = list(mayor_position_entities)
    mayor_superclasses.append('Q99356295')
    vl = format_position_iterable(mayor_superclasses)
    query = """select ?q ?j ?l ?p ?t {
        {
                ?q wdt:P279 ?p;
                        wdt:P1001 ?j.
                values ?p { %s }
                ?j wdt:P17 wd:Q213;
                        rdfs:label ?l.
                filter(lang(?l) = "cs")
        } union {
                ?t p:P31 ?s.
                ?s ps:P31 wd:Q15238777;
                  pq:P642 wd:Q2347172.
                optional { ?t p:P156 ?n. }
                filter(!bound(?n))
        }
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
        self.recent_year = today.year - 3
        self.year_precision = 9

        self.name_check = NameCheck()

        self.tree_check = TreeCheck()
        self.tree_check.add('titleBefore', 'mudr', Entity.physician)
        self.tree_check.add('titleBefore', 'mvdr', Entity.veterinarian)
        # MDDr. is also possible but rare, and wikidata doesn't have
        # the politically active dentists at all. Neither does
        # arch. match any new architects. PaedDr. causes false
        # positives - pedagogues require a school match, or some other
        # restriction...
        self.tree_check.add('titleBefore', 'ing', Entity.engineer)
        self.tree_check.add('titleAfter', 'mba', Entity.manager)

        self.city2mayor = {}
        self.last_legislature = None

        # hardcoded, for now - but Wikidata does have city assembly
        # member entitities for multiple Czech cities...
        self.city2councillor = {
            'praha': 'Q27830380',
            'brno': 'Q97482758',
        }

        # hardcoded list is not really satisfactory...
        self.name2city = {
            'magistrát hlavního města prahy': 'praha',
            'magistrát města české budějovice': 'české budějovice',
            'magistrát města chomutova': 'chomutov',
            'magistrát města ústí n.l.': 'ústí nad labem',
            'magistrát města mladá boleslav': 'mladá boleslav',
            'magistrát mladá boleslav': 'mladá boleslav',
            'magistrát města mostu': 'most',
            'magistrát města opavy': 'opava',
            'magistrát města pardubic': 'pardubice',
            'magistrát města plzně': 'plzeň',
            'magistrát města ústí n.l.': 'ústí',
            'statutární město brno - magistrát města brna': 'brno',
            'statutární město karviná, magistrát města karviné': 'karviná',
            'statutární město ostrava, magistrát města ostravy': 'ostrava',
            'úřad městské části města brna, brno-komín': 'brno',
            'úřad městské části brno-střed': 'brno',
            'úřad městské části brno-vinohrady': 'brno',
            'úřad městské části praha': 'praha',
            'úřad městské části praha - újezd': 'praha',
            'úřad městské části praha-troja': 'praha',
            'úřad městského obvodu pardubice vi': 'pardubice',
            'úřad městského obvodu plzeň 1': 'plzeň',
            'úřad městského obvodu ústí nad labem - neštěmice': 'ústí',
        }

        # Does not include district & regional courts - for those we
        # want a negative match. Values must be substrings of
        # genitive.
        self.court2stem = {
            'nejvyšší soud': 'nejvyššího soudu', # do not match nejvyšší správní soud - that's considered non-prominent
            'ústavní soud': 'ústavní', # ústavního; note the specific value is also used below
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

        cur.execute("""select wd_entity
from cro_last_legislature
where id=1""")
        row = cur.fetchone()
        if row:
            self.last_legislature = row[0]

    def store(self, cur):
        for city, mayor in self.city2mayor.items():
            cur.execute("""insert into cro_mayor_of(wd_entity, municipality)
values(%s, %s)
on conflict(wd_entity) do update
set municipality=%s""", (mayor, city, city))

        if self.last_legislature:
            cur.execute("""insert into cro_last_legislature(id, wd_entity)
values(1, %s)
on conflict(id) do update
set wd_entity=%s""", (self.last_legislature, self.last_legislature))

    def add_muni_mayor(self, city, mayor):
        norm_muni = self.normalize_city(city)
        if len(norm_muni) > 1:
            self.city2mayor[norm_muni] = mayor

    def add_last_legislature(self, leg_ent):
        self.last_legislature = leg_ent

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

    def make_person_names(self, detail):
        names = self.name_check.walk(detail)
        if not len(names):
            raise Exception("Person detail has no name")

        return names

    def make_position_set(self, detail):
        sought = set()

        # will probably (but not provably) also be handled by
        # rulebook; Entity.judge should work outside rulebook (unlike
        # e.g. Entity.mp) because judges w/o JudgeLevel matches are
        # considered non-prominent and get a negative court set
        if detail['judge']:
            sought.add(Entity.judge)

        entities = self.tree_check.find(detail)
        sought |= entities

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

        school_set = self.find_schools(detail)
        if len(school_set):
            sought.add(Entity.pedagogue)
            # could also add other entities here, but they aren't
            # special-cased in make_query_urls...

        return sought

    def find_schools(self, detail):
        school_set = set()
        for statement in detail['statements']:
            incomes = statement.get('incomes')
            if incomes:
                for income in incomes:
                    legal = income.get('legalPerson')
                    if legal:
                        school_name = check_school(legal.get('name'))
                        if school_name:
                            school_set.add(school_name)

            others = statement.get('otherContracts')
            if others:
                for other in others:
                    tp = other.get('type')
                    if tp and tp.get('type') == 'EMPLOYMENT_RELATIONSHIP':
                        school_name = check_school(other.get('name'))
                        if school_name:
                            school_set.add(school_name)

        return school_set

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

    def make_query_urls(self, detail, position_set):
        name_cond = format_name_set(self.make_person_names(detail))

        specific = len(position_set)
        position_list = list(position_set)

        minister_position = None
        if Entity.minister in position_set:
            position_set.remove(Entity.minister)
            minister_position = Entity.minister

        office_of_government_flag = False
        if Entity.head_of_office_of_government in position_set:
            position_set.remove(Entity.head_of_office_of_government)
            office_of_government_flag = True

        mp_position = None
        if Entity.mp in position_set:
            position_set.remove(Entity.mp)
            mp_position = Entity.mp

        ambassador_position = None
        if Entity.ambassador in position_set:
            position_set.remove(Entity.ambassador)
            ambassador_position = Entity.ambassador

        judge_position = None
        court_set = None
        if Entity.judge in position_set:
            position_set.remove(Entity.judge)
            judge_position = Entity.judge
            court_set = self.make_court_set(detail)

        prosecutor_flag = False
        for pos in (Entity.prosecutor, Entity.state_attorney):
            if pos in position_set:
                position_set.remove(pos)
                prosecutor_flag = True

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
        if prosecutor_flag:
            occupation_list.append(Entity.prosecutor)
            occupation_list.append(Entity.state_attorney)

        hygienist_flag = False
        if Entity.hygienist in position_set:
            position_set.remove(Entity.hygienist)
            occupation_list.append(Entity.hygienist)
            hygienist_flag = True

        # doctors by themselves produce false positives, so we always
        # combine them with other occupations...
        physician_flag = False
        if Entity.physician in position_set:
            position_set.remove(Entity.physician)
            # ...except for hygienists; a hygienist may or may not be
            # MUDr., but even if they are, wikidata usually don't list
            # them as physician...
            if not hygienist_flag:
                occupation_list.append(Entity.politician)
                physician_flag = True

        engineer_flag = False
        if Entity.engineer in position_set:
            position_set.remove(Entity.engineer)
            # Not added to occupation_list. For judges, we don't care
            # whether they're Ing.
            if not judge_position:
                engineer_flag = True

        for occupation in (Entity.diplomat, Entity.police_officer, Entity.psychiatrist, Entity.veterinarian, Entity.archaeologist, Entity.academic, Entity.researcher, Entity.university_teacher, Entity.manager):
            if occupation in position_set:
                position_set.remove(occupation)
                occupation_list.append(occupation)

        school_names = set()
        if Entity.pedagogue in position_set:
            position_set.remove(Entity.pedagogue)
            school_names = reduce_substrings_to_shortest(self.find_schools(detail))
            # teachers w/o school are treated like other occupations
            # (until they cause false positives, at which point
            # they'll be removed from whatever's producing them)
            if not len(school_names):
                occupation_list.append(Entity.pedagogue)

        l0 = len(occupation_list)
        if len(school_names):
            l0 += 1

        pos_clauses = []
        if minister_position:
            np = 'wd:' + minister_position
            # e.g. Q25515749 (Minister for Regional Development) is a
            # direct subclass of minister...
            pos_clauses.append(wrap_pos_clause('?p wdt:P279 %s.' % np, l0))
            # ...while Q25507811 (Minister of Industry and Trade) is a
            # subclass of industry and commerce ministers...
            pos_clauses.append(wrap_pos_clause('?p wdt:P279/wdt:P279 %s.' % np, l0))

        if office_of_government_flag:
            np = 'wd:' + Entity.head_of_office_of_government
            np2 = 'wd:' + 'Q11089595'
            # never wrapped
            pos_clauses.append("""{
                ?w wdt:P39 %s.
        } union {
                %s wdt:P1308 ?w.
        }""" % (np, np2))

        if ambassador_position:
            np = 'wd:' + ambassador_position
            pos_clauses.append(wrap_pos_clause('?p wdt:P279 %s.' % np, l0))

        if mp_position:
            min_year = self.fold_min_start(detail, mp_position)
            # ?t is already taken; 'coalesce(?f, ?u) >=
            # "%d-01-01"^^xsd:dateTime' might be more efficient...
            base_cond = 'year(coalesce(?f, ?u)) > %d' % (min_year - 1)
            if self.last_legislature:
                np = 'wd:' + self.last_legislature
                base_cond += ' || ?e = %s' % np

            mp_values = format_position_iterable((mp_position, Entity.mp_speaker))
            extra_clause = """values ?p { %s }
        optional { ?w p:P39/pq:P580 ?f. }
        optional { ?w p:P39/pq:P582 ?u. }
        optional { ?w p:P39/pq:P2937 ?e. }
        filter(%s)""" % (mp_values, base_cond)
            pos_clauses.append(wrap_pos_clause(extra_clause, l0))

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
            # ignoring office_of_government_flag in this branch -
            # hopefully there'll be no overlap...
            if not l:
                political_constraint = 'wdt:P106 ?o;'
                mainline_block = 'optional { ?w wdt:P39 ?p. }'
            else:
                political_constraint = 'wdt:P39 ?p; wdt:P106 ?o;'

            judge_positions = [ judge_position ]
            if 'ústavní' in court_set:
                judge_positions.append(Entity.constitutional_judge)

            vl = format_position_iterable(judge_positions)
            mainline_block += 'values ?o { %s }' % vl
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
                    assert not office_of_government_flag
                    if not prosecutor_flag:
                        political_constraint = 'wdt:P106 ?o;'
                        mainline_block = 'optional { ?w wdt:P39 ?p. }'
                    else:
                        loc_occ = True
                else:
                    if office_of_government_flag:
                        mainline_block = 'optional { ?w wdt:P39 ?p. }'

                    loc_occ = True

        if len(occupation_list):
            occ_branch = []
            if loc_occ:
                if physician_flag:
                    np = 'wd:' + Entity.physician
                    occ_branch.append('?w wdt:P106 %s, ?o.' % np)
                else:
                    occ_branch.append('?w wdt:P106 ?o.')

            vl = format_position_iterable(occupation_list)
            occ_branch.append('values ?o { %s }' % vl)

            if physician_flag and not loc_occ:
                np = 'wd:' + Entity.physician
                occ_branch.append('?w wdt:P106 %s.' % np)

            pos_clauses.append(''.join(occ_branch))

            # prosecutor already is in occupation_list (and therefore
            # in pos_clauses), but the occupation almost never matches
            # - so we also try to match description...
            if prosecutor_flag:
                pos_clauses.append('filter(contains(lcase(?g), "státní zástup"))') # zástupce, zástupkyně

        # engineers (unlike doctors, whose titles are a more prominent
        # part of their identity) get a separate query
        if engineer_flag:
            np = 'wd:' + Entity.engineer
            np2 = 'wd:' + Entity.politician
            occ_tail = ', ?o' if loc_occ else ''
            eng_occ = """?w wdt:P106 %s%s.
        values ?o { %s }""" % (np, occ_tail, np2)
            pos_clauses.append(eng_occ)

        if len(school_names):
            occ_clause = '?w wdt:P106 ?o.' if loc_occ else ''
            teacher_entities = (Entity.pedagogue, Entity.teacher)
            teacher_values = format_position_iterable(teacher_entities)
            school_expr = format_school_set(school_names)
            teacher_occ = """%svalues ?o { %s }
        ?w wdt:P108/rdfs:label ?k.
        filter(lang(?k) = "cs" && %s)""" % (occ_clause, teacher_values, school_expr)
            pos_clauses.append(teacher_occ)

        l = len(pos_clauses)

        # combining the conditions into the main (name) filter slows
        # down download more than 3x...
        death_clause = """optional { ?w wdt:P570 ?d. }
        filter(!bound(?d) || year(?d) > %d)""" % (self.recent_year - 1)

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

        if l == 0:
            # no restriction; can happen even when the original
            # position set is non-empty - if that causes false
            # positives, we'll have to revisit...
            pos_clauses.append("")

        # it would be much cleaner to have a single query per source
        # doc, but they're getting too complex for wikidata, so the
        # current model is a list of queries, locally combined into a
        # union
        urls = []
        for pos_clause in pos_clauses:
            # person (wikidata ID), article, birth date, birth date
            # precision, label, general description, position,
            # occupation
            query = """select ?w ?a ?b ?n ?l ?g ?p ?o {
        ?w wdt:P27 wd:Q213;
                rdfs:label ?l;
                %s
                p:P569/psv:P569 [ wikibase:timeValue ?b; wikibase:timePrecision ?n ].
        %s%s
        optional {
                ?a schema:about ?w;
                        schema:inLanguage "cs".
        }
        optional {
                ?w schema:description ?g.
                filter(lang(?g) = "cs")
        }
        filter(?n > %d && lang(?l) = "cs" && %s%s)
        %s
}""" % (political_constraint, death_clause, mainline_block, self.year_precision - 1, name_cond, judge_cond, pos_clause)
            urls.append(create_query_url(query))

        return urls

    def make_query_single_url(self, detail, position_set):
        urls = self.make_query_urls(detail, position_set)
        if len(urls) != 1:
            raise Exception("got %d urls when 1 expected" % len(urls))

        return urls[0]

if __name__ == "__main__":
    # needed by seed.sh
    print(make_meta_query_url())
