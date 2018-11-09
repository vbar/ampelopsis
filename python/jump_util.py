from urllib.parse import quote
import re
from common import space_rx

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")

city_start_rx = re.compile("^(?:mč|město|městská část|obec|statutární město) ")

university_name_rx = re.compile("\\b(?:univerzita|učení)")

# ministers are special because their position is a subclass of
# minister - normally minister of <resort> of Czech Republic
minister_position_entity = 'Q83307'

# mayors are special because there's so many the name match may
# produce false positives - so for mayors we also match the city
mayor_position_entities = ( 'Q30185', 'Q147733' )

# councillors are even commoner than mayors, but in practice, wikidata
# doesn't have the same statements for councillors as for mayors - the
# stricter check is useful primarily for removing false positives...
councillor_position_entities = ( 'Q708492', 'Q19602879', 'Q4657217' )

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

def make_position_set(detail):
    university2rector = {
        'Univerzita Karlova': 'Q12049166',
        'Univerzita Karlova v Praze': 'Q12049166'
    }

    sought = set()
    lst = detail['workingPositions']
    for it in lst:
        org_name = it['organization'].strip()
        if org_name == 'Nejvyšší státní zastupitelství':
            sought.add('Q26197430')

        wp = it['workingPosition']
        if wp['name'] == 'člen řídícího orgánu':
            rector = university2rector.get(org_name)
            if rector:
                sought.add(rector)

            if rector or university_name_rx.search(org_name.lower()):
                sought.add('Q212071')
                sought.add('Q2113250')
                sought.add('Q723682')

        if wp['name'] == 'vedoucí zaměstnanec 3. stupně řízení':
            if (org_name == 'Kancelář prezidenta republiky'):
                sought.add('Q15712674')
            else:
                sought.add('Q1162163')

        if wp['name'] == 'člen vlády':
            # currently matching only ministers, but 'člen vlády' is
            # also used for (at least some) deputy ministers...
            sought.add(minister_position_entity)
        elif wp['name'] == 'náměstek pro řízení sekce':
            sought.add('Q15735113')
        elif wp['name'] == 'starosta':
            for pos in mayor_position_entities:
                sought.add(pos)
        elif wp['name'] == 'místostarosta / zástupce starosty':
            sought.add('Q581817')
        elif wp['name'] in ( 'člen zastupitelstva', 'člen Rady' ):
            for pos in councillor_position_entities:
                sought.add(pos)
        elif wp['name'] == 'člen bankovní rady České národní banky': # missing the governor
            sought.add('Q28598459')
        elif (wp['name'] == 'soudce'):
            sought.add('Q16533')
        elif (wp['name'] == 'ředitel bezpečnostního sboru'):
            sought.add('Q1162163')
        elif wp['name'] == 'vedoucí zastupitelského úřadu':
            sought.add('Q121998')

        if (wp['name'] == 'poslanec') or wp['deputy']:
            sought.add('Q1055894')
            sought.add('Q19803234') # should be a subset but isn't
            sought.add('Q486839')

        if (wp['name'] == 'senátor') or wp['senator']:
            sought.add('Q15686806')
            sought.add('Q18941264')
            sought.add('Q486839')

    return sought

def make_city_set_for_mayor(detail):
    sought = set()
    lst = detail['workingPositions']
    for it in lst:
        wp = it['workingPosition']
        if wp['name'] == 'starosta':
            sought.add(normalize_city(it['organization']))

    return sought

def make_city_set_for_councillor(detail):
    sought = set()
    lst = detail['workingPositions']
    for it in lst:
        wp = it['workingPosition']
        if wp['name'] in ( 'člen zastupitelstva', 'člen Rady' ):
            sought.add(normalize_city(it['organization']))

    return sought

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

    mayor_position_set = set()
    for pos in mayor_position_entities:
        if pos in position_list:
            position_set.remove(pos)
            mayor_position_set.add(pos)

    councillor_position_set = set()
    for pos in councillor_position_entities:
        if pos in position_list:
            position_set.remove(pos)
            councillor_position_set.add(pos)

    minister_position = None
    if minister_position_entity in position_set:
        position_set.remove(minister_position_entity)
        minister_position = minister_position_entity

    pos_clauses = []
    if minister_position:
        nmp = 'wd:' + minister_position
        pos_clauses.append('?p wdt:P279/wdt:P279 %s.' % nmp)

    if len(mayor_position_set):
        city_set = make_city_set_for_mayor(detail)
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

    if len(councillor_position_set):
        city_set = make_city_set_for_councillor(detail)
        if len(city_set):
            vl = ' '.join('wd:' + p for p in sorted(councillor_position_set))

            filter_expr = ' || '.join('(lcase(?t) = "%s")' % c for c in sorted(city_set))

            # city (or village), title (of city - ?l is already taken)
            bare_clause = """values ?p { %s }
        ?c p:P6/ps:P6 ?w.
        ?c rdfs:label ?t.
        filter(lang(?t) = "cs").
        filter(%s).""" % (vl, filter_expr)
            pos_clauses.append(bare_clause)

    if len(position_set):
        vl = ' '.join('wd:' + p for p in sorted(position_set))
        pos_clauses.append('values ?p { %s }' % vl)

    l = len(pos_clauses)
    if l == 0:
        pos_clause = '' # no restriction
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
