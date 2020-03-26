#!/usr/bin/python3

from urlize import create_query_url

def make_meta_query_url():
    # not needed for anything but hostname whitelisting yet
    query = """select ?c {
  ?c wdt:P31 wd:Q146.
}
limit 2"""
    return create_query_url(query)


def make_personage_query_url(person):
    query = """select ?w ?l ?p ?t {
  ?w wdt:P27 wd:Q213;
     wdt:P106 wd:Q82955;
     rdfs:label ?l;
     p:P102 ?s;
     wdt:P569 ?b.
  ?s ps:P102 ?p.
  filter(lang(?l) = "cs" && contains(lcase(?l), "%s") && year(?b) = %d)
  minus { ?s pq:P582 ?e. }
  ?p rdfs:label ?t.
  filter(lang(?t) = "cs")
}""" % (person.query_name, person.birth_year)
    return create_query_url(query)


if __name__ == "__main__":
    # needed by seed.sh
    print(make_meta_query_url())
