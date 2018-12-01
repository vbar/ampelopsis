import re
from urllib import parse
from common import space_rx

query_url_head = "https://query.wikidata.org/sparql?format=json&query="

whitespace_rx = re.compile("\\s+")

def normalize_url_param(path):
    q = parse.quote(path)
    return space_rx.sub('+', q)

def create_query_url(query):
    mq = whitespace_rx.sub(" ", query.strip())
    return query_url_head + normalize_url_param(mq)

def extract_query(qurl):
    uo = parse.urlparse(qurl)
    params = parse.parse_qsl(uo.query)
    for p in params:
        if p[0] == 'query':
            return p[1]

    return None
