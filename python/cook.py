import re
from personage import normalize_name
from urlize import create_query_url

# also matches ministryně; hopefully the input has no ministrants...
minister_rx = re.compile("\\bministr")

def make_query(core, query_name):
    query = "select ?w ?l ?b ?o ?p ?t ?c ?z ?f ?u{\n"
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


def make_speaker_query_urls(position, name):
    # nationality check is too narrow for guest speakers, but we won't
    # get / really need a party for foreigners anyway...
    pol_tmpl = """?w wdt:P27 wd:Q213;
        wdt:P39 ?o;
        p:P102 ?s.
    %s
    ?s ps:P102 ?p.
    optional { ?s pq:P580 ?f. }
    optional { ?s pq:P582 ?u. }
"""

    norm_pos = position.lower()
    query_name = normalize_name(name)
    queries = []
    if minister_rx.match(norm_pos):
        np = 'wd:Q83307'
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

    return [ create_query_url(query) for query in queries ]
