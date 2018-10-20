from urllib.parse import quote
import re
from common import space_rx

search_url_head = "https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&language=cs&search="

# not the same as in common because it also needs to reflect curl
# canonicalization...
def normalize_url_component(path):
    q = quote(path)
    return space_rx.sub('+', q)

def make_search_url(first_name, last_name):
    name = "%s %s" % (first_name.strip(), last_name.strip())
    return search_url_head + normalize_url_component(name)

def make_query_url(wid):
    # position, occupation, birth
    query = """select ?p ?o ?b
where {
        wd:%s wdt:P27 wd:Q213;
                wdt:P39 ?p;
                wdt:P106 ?o;
                wdt:P569 ?b.
}""" % wid
    mq = re.sub("\\s+", " ", query.strip())
    return "https://query.wikidata.org/sparql?format=json&query=" + normalize_url_component(mq)
