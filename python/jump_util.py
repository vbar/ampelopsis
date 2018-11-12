from urllib.parse import quote
import re
from common import space_rx
from rulebook import CityLevel, councillor_position_entities, deputy_mayor_position_entity, get_org_name, judge_position_entity, mayor_position_entities, minister_position_entity, rulebook

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

city_start_rx = re.compile("^(?:mč|město|městská část|městys|obec|obecní úřad|statutární město) ")

def normalize_name(name):
    return name_char_rx.sub("", name.strip())

# not the same as in common because it also needs to reflect curl
# canonicalization...
def normalize_url_component(path):
    q = quote(path)
    return space_rx.sub('+', q)

def normalize_city(name):
    lst = re.split('[,;-]', name, maxsplit=1) # '-' will split Frýdek-Místek, but SPARQL queries match on string start anyway...
    lower = lst[0].lower()
    safer = name_char_rx.sub("", lower.strip()) # maybe we need a more permissive regex, but nothing specific comes to mind...
    shorter = city_start_rx.sub("", safer)
    return shorter.strip()

def convert_answer_to_iterable(answer, it):
    if callable(answer): # technically we could have a cycle, but hopefully nobody will need that...
        answer = answer(it)

    if isinstance(answer, str):
        return (answer,)
    else: # must be iterable
        return answer

def make_position_set(detail):
    sought = set()

    # will probably (but not provably) also be handled by rulebook
    if detail['judge']:
        sought.add(judge_position_entity)

    lst = detail['workingPositions']
    for it in lst:
        org_name = get_org_name(it)
        if org_name == 'Nejvyšší státní zastupitelství':
            sought.add('Q26197430')

        wp = it['workingPosition']
        wp_name = wp['name']
        answer = rulebook.get(wp_name)
        if answer:
            answer = convert_answer_to_iterable(answer, it)
            for pos in answer:
                sought.add(pos)

        # probably synonymous, and could be included in rulebook, but
        # just to play it safe...
        if (wp_name == 'poslanec') or wp['deputy']:
            sought.add('Q1055894')
            sought.add('Q19803234') # should be a subset but isn't
            sought.add('Q486839')
        elif (wp_name == 'senátor') or wp['senator']:
            sought.add('Q15686806')
            sought.add('Q18941264')
            sought.add('Q486839')

    return sought

def make_city_set(detail, representative):
    sought = set()
    lst = detail['workingPositions']
    for it in lst:
        wp = it['workingPosition']
        answer = rulebook.get(wp['name'])
        if answer is not None and isinstance(answer, CityLevel):
            answer = convert_answer_to_iterable(answer, it)
            if representative in answer:
                sought.add(normalize_city(it['organization']))

    return sought

def format_councillor_bare_clause(councillor_position_iterable, city_set):
    vl = ' '.join('wd:' + p for p in sorted(councillor_position_iterable))

    # equality should be sufficient but actually doesn't match some labels
    filter_expr = ' || '.join('strstarts(lcase(?t), "%s")' % c for c in sorted(city_set))

    return """values ?p { %s }
        ?w p:P39/pq:P642 ?c.
        ?c rdfs:label ?t.
        filter(lang(?t) = "cs").
        filter(%s).""" % (vl, filter_expr)

def make_query_url(detail, position_set):
    city2mayor = {
        'brno': 'Q28860819',
        'litovel': 'Q32086537',
        'praha': 'Q17149373',
        'příbor': 'Q29071925',
        'slaný': 'Q42968448',
        'třebíč': 'Q28860110',
    }

    # city2councillor is possible, with 'praha': 'Q27830380', but may not match anyone...

    name = "%s %s" % tuple(normalize_name(detail[n]) for n in ('firstName', 'lastName'))
    name_clause = 'filter(contains(?l, "%s")).' % name

    position_list = list(position_set)

    minister_position = None
    if minister_position_entity in position_set:
        position_set.remove(minister_position_entity)
        minister_position = minister_position_entity

    judge_position = None
    if judge_position_entity in position_set:
        position_set.remove(judge_position_entity)
        judge_position = judge_position_entity

    mayor_position_set = set()
    for pos in mayor_position_entities:
        if pos in position_list:
            position_set.remove(pos)
            mayor_position_set.add(pos)

    deputy_mayor_position = None
    if deputy_mayor_position_entity in position_set:
        position_set.remove(deputy_mayor_position_entity)
        deputy_mayor_position = deputy_mayor_position_entity

    councillor_position_set = set()
    for pos in councillor_position_entities:
        if pos in position_list:
            position_set.remove(pos)
            councillor_position_set.add(pos)

    pos_clauses = []
    if minister_position:
        np = 'wd:' + minister_position
        pos_clauses.append('?p wdt:P279/wdt:P279 %s.' % np)

    if len(mayor_position_set):
        city_set = make_city_set(detail, mayor_position_entities[0])
        for city in city_set:
            mayor = city2mayor.get(city)
            if mayor:
                position_set.add(mayor)

        if len(city_set):
            vl = ' '.join('wd:' + p for p in sorted(mayor_position_set))

            # equality should be sufficient but actually doesn't match some labels
            filter_expr = ' || '.join('strstarts(lcase(?t), "%s")' % c for c in sorted(city_set))

            # city (or village), title (of city - ?l is already taken)
            bare_clause = """values ?p { %s }
        ?c p:P6/ps:P6 ?w.
        ?c rdfs:label ?t.
        filter(lang(?t) = "cs").
        filter(%s).""" % (vl, filter_expr)
            pos_clauses.append(bare_clause)

    deputy_mayor_city_set = set()
    if deputy_mayor_position:
        # Constructing city set separately for deputy mayor actually
        # leads to fewer matches - maybe it's just a time mismatch,
        # and we should do it (like for bank governor)? OTOH there's
        # more municipal councillors than central bank's - let's
        # distinguish, for now...
        deputy_mayor_city_set = make_city_set(detail, deputy_mayor_position)

    councillor_city_set = set()
    if len(councillor_position_set):
        councillor_city_set = make_city_set(detail, councillor_position_entities[0])

    # deputy mayor and councillor have the same bare clause; if their
    # city set is the same, they can be combined
    if len(deputy_mayor_city_set) and len(councillor_city_set) and (deputy_mayor_city_set == councillor_city_set):
        assert deputy_mayor_position
        assert len(councillor_position_set)
        councillor_city_set |= deputy_mayor_city_set
        deputy_mayor_city_set = set()
        councillor_position_set.add(deputy_mayor_position)
        deputy_mayor_position = None

    if len(deputy_mayor_city_set):
        assert deputy_mayor_position
        pos_clauses.append(format_councillor_bare_clause((deputy_mayor_position,), deputy_mayor_city_set))

    if len(councillor_city_set):
        assert len(councillor_position_set)
        pos_clauses.append(format_councillor_bare_clause(councillor_position_set, councillor_city_set))

    if len(position_set):
        vl = ' '.join('wd:' + p for p in sorted(position_set))
        pos_clauses.append('values ?p { %s }' % vl)

    l = len(pos_clauses)

    # judge is such a specific feature we require it when present in
    # input data (rather than or-ing it with political positions)
    occupation_filter = ''
    if judge_position:
        np = 'wd:' + judge_position
        if not l:
            # we can reuse ?p
            political_constraint = 'wdt:P106 ?p;'
            occupation_filter = 'values ?p { %s }' % np
        else:
            # leave ?p alone, add non-parametrized rule
            political_constraint = 'wdt:P39 ?p; wdt:P106 %s;' % np
    else:
        political_constraint ='wdt:P39 ?p;'

    if l == 0:
        # no restriction (unless judge); can happen even when the
        # original position set is non-empty, and if it causes false
        # positives, we'll have to revisit...
        pos_clause = occupation_filter
    elif l == 1:
        pos_clause = pos_clauses[0]
    else:
        pos_clause = ' union '.join('{ %s }' % pc for pc in pos_clauses)

    # person (wikidata ID), article, birth, label, position
    query = """select ?w ?a ?b ?l ?p
where {
        ?w wdt:P27 wd:Q213;
                rdfs:label ?l;
                %s
                wdt:P569 ?b.
        ?a schema:about ?w.
        ?a schema:inLanguage "cs".
        filter(lang(?l) = "cs").
        %s %s
}""" % (political_constraint, name_clause, pos_clause)
    mq = re.sub("\\s+", " ", query.strip())
    return "https://query.wikidata.org/sparql?format=json&query=" + normalize_url_component(mq)
