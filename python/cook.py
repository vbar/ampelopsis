import re
from personage import normalize_name
from urlize import create_query_url

# also matches ministryně; hopefully the input has no ministrants...
minister_rx = re.compile("\\bministr")

# also matches senátorka
senator_rx = re.compile("\\bsenátor")

minister_position = 'Q83307'

senator_position = 'Q18941264'

def make_query(core, query_name):
    query = "select ?w ?l ?b ?o ?d ?e ?p ?t ?c ?z ?f ?u{\n"
    query += core
    query += """
    ?w rdfs:label ?l;
        p:P569/psv:P569 [ wikibase:timeValue ?b; wikibase:timePrecision ?n ].
    filter(lang(?l) = "cs" && contains(lcase(?l), \"%s\") && ?n > 9)
    ?p rdfs:label ?t.
    filter(lang(?t) = "cs")
    optional { ?p wdt:P465 ?c. }
    optional {
        ?p wdt:P1813 ?z.
        filter(lang(?z) = "cs")
    }
}""" % (query_name,)

    return query


def make_speaker_position_set(position):
    position_set = set()
    norm_pos = position.lower()
    if minister_rx.match(norm_pos):
        position_set.add(minister_position)

    if senator_rx.match(norm_pos):
        position_set.add(senator_position)

    return position_set


def make_speaker_query_urls(name, position_set):
    # nationality check is too narrow for guest speakers, but we won't
    # get / really need a party for foreigners anyway...
    pol_tmpl = """?w wdt:P27 wd:Q213;
        p:P39 ?r;
        p:P102 ?s.
    ?r ps:P39 ?o.
    %s
    ?s ps:P102 ?p.
    optional { ?r pq:P580 ?d. }
    optional { ?r pq:P582 ?e. }
    optional { ?s pq:P580 ?f. }
    optional { ?s pq:P582 ?u. }
"""

    query_name = normalize_name(name)
    queries = []
    if minister_position in position_set:
        np = 'wd:' + minister_position
        pos_clauses = []

        # e.g. Q25515749 (Minister for Regional Development) is a
        # direct subclass of minister...
        pos_clauses.append('?o wdt:P279 %s.' % np)

        # ...while Q25507811 (Minister of Industry and Trade) is a
        # subclass of industry and commerce ministers...
        pos_clauses.append('?o wdt:P279/wdt:P279 %s.' % np)

        minister_clause = "union".join(( "{ %s }" % c for c in pos_clauses ))
        minister_core = pol_tmpl % minister_clause
        queries.append(make_query(minister_core, query_name))

    if senator_position in position_set:
        np = 'wd:' + senator_position
        senator_clause = "values ?o { %s }" % np
        senator_core = pol_tmpl % senator_clause
        queries.append(make_query(senator_core, query_name))

    return [ create_query_url(query) for query in queries ]
