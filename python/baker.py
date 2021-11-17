#!/usr/bin/python3

from urlize import create_query_url

def make_query(core, person):
    query = "select ?w ?l ?p ?t ?c ?z"
    if not person:
        query += " ?g ?b"
    else:
        query += " ?f ?u"

    query += "{\n"
    query += core
    query += """
    ?w rdfs:label ?l;
        wdt:P569 ?b.
    filter(lang(?l) = "cs"
"""

    if person:
        query += "&& contains(lcase(?l), \"%s\") && year(?b) = %d" % (person.query_name, person.birth_year)

    query += """)
    ?p rdfs:label ?t.
    filter(lang(?t) = "cs")
    optional { ?p wdt:P465 ?c. }
    optional {
        ?p wdt:P1813 ?z.
        filter(lang(?z)="cs")
    }
}"""

    return query


def make_meta_query_url():
    # only checking governments of Czech Republic - if we ever get
    # back to Czechoslovakia this'll need to be extended
    gov_core = """?g wdt:P31 wd:Q5015587.
    ?g p:P527 ?s.
    ?s ps:P527 ?w;
    pq:P4353 ?p.
"""

    return create_query_url(make_query(gov_core, None))


def make_personage_query_urls(person):
    pol_core = """?w wdt:P27 wd:Q213;
        wdt:P106 wd:Q82955;
        p:P102 ?s.
    ?s ps:P102 ?p.
    optional { ?s pq:P580 ?f. }
    optional { ?s pq:P582 ?u. }
"""

    mp_core = """?w wdt:P27 wd:Q213;
        wdt:P106 wd:Q82955;
        p:P4100 ?s.
    {
        ?s ps:P4100/p:P31/pq:P642 ?p.
    } union {
        ?s ps:P4100/wdt:P102 ?p.
    }
    optional { ?s pq:P580 ?f. }
    optional { ?s pq:P582 ?u. }
"""

    assert person
    queries = (
        make_query(pol_core, person),
        make_query(mp_core, person)
    )

    return [ create_query_url(query) for query in queries ]


if __name__ == "__main__":
    # needed by seed
    print(make_meta_query_url())
