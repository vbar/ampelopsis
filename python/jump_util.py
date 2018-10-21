from urllib.parse import quote
import re
from common import space_rx

# not the same as in common because it also needs to reflect curl
# canonicalization...
def normalize_url_component(path):
    q = quote(path)
    return space_rx.sub('+', q)

def make_query_url(first_name, last_name):
    name = "%s %s" % (first_name.strip(), last_name.strip())

    # article, birth, label, position
    query = """select ?a ?b ?l ?p
where {
        ?w wdt:P27 wd:Q213;
                rdfs:label ?l;
                wdt:P39 ?p;
                wdt:P569 ?b.
        ?a schema:about ?w.
        ?a schema:inLanguage "cs".
        filter(lang(?l) = "cs").
        filter(contains(?l, "%s")).
}""" % name
    mq = re.sub("\\s+", " ", query.strip())
    return "https://query.wikidata.org/sparql?format=json&query=" + normalize_url_component(mq)
