#!/usr/bin/python3

from urlize import create_query_url

KERNEL = 1

WOOD = 2

ALL = KERNEL | WOOD

def make_query(core, wood_flag, person):
    query = "select"
    if wood_flag:
        query += " ?w ?l ?p ?t ?c ?z"
        if person:
            query += " ?f ?u"
        else:
            query += " ?g ?b"
    else:
        query += " ?w ?l"

    query += "{\n"
    query += core
    query += """
    ?w rdfs:label ?l;
        wdt:P569 ?b.
    filter(lang(?l) = "cs"
"""

    if person:
        query += "&& contains(lcase(?l), \"%s\") && year(?b) = %d" % (person.query_name, person.birth_year)

    query += ")"

    if wood_flag:
        query += """
    ?p rdfs:label ?t.
    filter(lang(?t) = "cs")
    optional { ?p wdt:P465 ?c. }
    optional {
        ?p wdt:P1813 ?z.
        filter(lang(?z)="cs")
    }"""

    query += "}"

    return query


def make_meta_query_url():
    # only checking governments of Czech Republic - if we ever get
    # back to Czechoslovakia this'll need to be extended
    gov_core = """?g wdt:P31 wd:Q5015587.
    ?g p:P527 ?s.
    ?s ps:P527 ?w;
    pq:P4353 ?p.
"""

    return create_query_url(make_query(gov_core, True, None))


def make_personage_query_urls(person, level):
    # no longer requiring wd:Q82955 - some politicians (that are party
    # members) do not have it...
    kernel_core = """?w wdt:P27 wd:Q213.
"""

    pol_core = """?w wdt:P27 wd:Q213;
        p:P102 ?s.
    ?s ps:P102 ?p.
    optional { ?s pq:P580 ?f. }
    optional { ?s pq:P582 ?u. }
"""

    mp_core = """?w wdt:P27 wd:Q213;
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
    queries = []
    if level & KERNEL:
        queries.append(make_query(kernel_core, False, person))

    if level & WOOD:
        queries.append(make_query(pol_core, True, person))
        queries.append(make_query(mp_core, True, person))

    return [ create_query_url(query) for query in queries ]


if __name__ == "__main__":
    # needed by seed
    print(make_meta_query_url())
