import re
from urllib import parse
from common import space_rx

query_url_head = "https://query.wikidata.org/sparql?format=json&query="

whitespace_rx = re.compile("\\s+")

# characters preceded and followed by space, unless they form a
# multi-character token (in which case the spaces are before and after
# the token) in regularized form that don't have the spaces in pressed
# form (because SPARQL doesn't need it)
token_rx_subset = "=<>!|&"

# characters preceded by space in regularized form that don't have the
# space in pressed form
open_rx_set = "[{}?%s]" % token_rx_subset

# characters followed by space in regularized form that don't have the
# space in pressed form; '.' isn't included because it's valid in
# string constants (inside name)
close_rx_set = "[{},;%s]" % token_rx_subset

open_rx = re.compile(" (%s)" % open_rx_set)

pressed_open_rx = re.compile("(?<![( %s])(%s)" % (token_rx_subset, open_rx_set))

close_rx = re.compile("(%s) " % close_rx_set)

pressed_close_rx = re.compile("(%s)(?![ %s])" % (close_rx_set, token_rx_subset))

def normalize_url_param(path):
    # could preserve parentheses (and braces) here, but
    # normalize_url_component would have to as well
    q = parse.quote(path, safe="/[]:")
    return space_rx.sub('+', q)

def create_query_url(query):
    mq = whitespace_rx.sub(" ", query.strip())
    half = open_rx.sub("\\1", mq)
    shorter = close_rx.sub("\\1", half)
    return query_url_head + normalize_url_param(shorter)

def reflate(q):
    half = pressed_close_rx.sub("\\1 ", q)
    reg = pressed_open_rx.sub(" \\1", half)
    return reg.strip()

def extract_query(qurl):
    uo = parse.urlparse(qurl)
    params = parse.parse_qsl(uo.query)
    for p in params:
        if p[0] == 'query':
            return reflate(p[1])

    return None
