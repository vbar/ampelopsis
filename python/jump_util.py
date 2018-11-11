from urllib.parse import quote
import re
from common import space_rx
from rulebook import CityLevel, councillor_position_entities, deputy_mayor_position_entity, get_org_name, mayor_position_entities, minister_position_entity, rule_book

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

city_start_rx = re.compile("^(?:mč|město|městská část|městys|obec|statutární město) ")

def normalize_name(name):
    return name_char_rx.sub("", name.strip())

# not the same as in common because it also needs to reflect curl
# canonicalization...
def normalize_url_component(path):
    q = quote(path)
    return space_rx.sub('+', q)

def normalize_city(name):
    lower = name.lower()
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
    lst = detail['workingPositions']
    for it in lst:
        org_name = get_org_name(it)
        if org_name == 'Nejvyšší státní zastupitelství':
            sought.add('Q26197430')

        wp = it['workingPosition']
        wp_name = wp['name']
        answer = rule_book.get(wp_name)
        if answer:
            answer = convert_answer_to_iterable(answer, it)
            for pos in answer:
                sought.add(pos)

        # probably synonymous, and could be included in rule_book, but
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
        answer = rule_book.get(wp['name'])
        if answer is not None and isinstance(answer, CityLevel):
            answer = convert_answer_to_iterable(answer, it)
            if representative in answer:
                sought.add(normalize_city(it['organization']))

    return sought

def format_councillor_bare_clause(detail, councillor_position_iterable):
    first = next(iter(councillor_position_iterable))
    city_set = make_city_set(detail, first)
    if not len(city_set):
        return None

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
        nmp = 'wd:' + minister_position
        pos_clauses.append('?p wdt:P279/wdt:P279 %s.' % nmp)

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

    if deputy_mayor_position:
        # constructing city set separately for deputy mayor actually
        # leads to fewer matches, but even if it works, matching
        # deputy mayor as councillor is incorrect...
        bare_clause = format_councillor_bare_clause(detail, (deputy_mayor_position,))
        if bare_clause:
            pos_clauses.append(bare_clause)

    if len(councillor_position_set):
        bare_clause = format_councillor_bare_clause(detail, councillor_position_set)
        if bare_clause:
            pos_clauses.append(bare_clause)

    if len(position_set):
        vl = ' '.join('wd:' + p for p in sorted(position_set))
        pos_clauses.append('values ?p { %s }' % vl)

    l = len(pos_clauses)
    if l == 0:
        # no restriction; can happen even when the original position
        # set is non-empty, and if it causes false positives, we'll
        # have to revisit...
        pos_clause = ''
    elif l == 1:
        pos_clause = pos_clauses[0]
    else:
        pos_clause = ' union '.join('{ %s }' % pc for pc in pos_clauses)

    # person (wikidata ID), article, birth, label, position
    query = """select ?w ?a ?b ?l ?p
where {
        ?w wdt:P27 wd:Q213;
                rdfs:label ?l;
                wdt:P39 ?p;
                wdt:P569 ?b.
        ?a schema:about ?w.
        ?a schema:inLanguage "cs".
        filter(lang(?l) = "cs").
        %s %s
}""" % (name_clause, pos_clause)
    mq = re.sub("\\s+", " ", query.strip())
    return "https://query.wikidata.org/sparql?format=json&query=" + normalize_url_component(mq)
