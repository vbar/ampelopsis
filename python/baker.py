#!/usr/bin/python3

from urlize import create_query_url

def make_query(core, person, has_from=False):
    query = "select ?w ?l ?p ?t ?c ?z"
    if not person:
        query += " ?b"

    if has_from:
        query += " ?f"

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
        filter(!bound(?z) || lang(?z)="cs")
    }
}"""

    return query


def make_meta_query_url():
    gov_core = """wd:Q55224909 p:P527 ?s.
    ?s ps:P527 ?w;
    pq:P4353 ?p.
"""

    return create_query_url(make_query(gov_core, None))


def make_personage_query_urls(person):
    # wdt:P576 works for KSC; for parties that technically still
    # exist, start year will be checked client-side
    pol_core = """?w wdt:P27 wd:Q213;
        wdt:P106 wd:Q82955;
        p:P102 ?s.
    ?s ps:P102 ?p.
    minus { ?s pq:P582 ?e. }
    minus { ?p wdt:P576 ?d. }
    optional { ?s pq:P580 ?f. }
"""

    mp_core = """?w wdt:P27 wd:Q213;
        wdt:P106 wd:Q82955;
        p:P4100 ?s.
    {
        ?s ps:P4100/p:P31/pq:P642 ?p.
    } union {
        ?s ps:P4100/wdt:P102 ?p.
    }
    minus { ?s pq:P582 ?e. }
"""

    assert person
    queries = (
        make_query(pol_core, person, True),
        make_query(mp_core, person, False)
    )

    return [ create_query_url(query) for query in queries ]


if __name__ == "__main__":
    # needed by seed.sh
    print(make_meta_query_url())
