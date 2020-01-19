import re
from urllib import parse
from common import space_rx

query_url_head = "https://query.wikidata.org/sparql?format=json&query="

whitespace_rx = re.compile("\\s+")

# characters preceded and followed by space, unless they form a
# multi-character token (in which case the spaces are before and after
# the token) in regularized form that don't have the spaces in pressed
# form (because SPARQL doesn't need it)
token_rx_subset = "=<>|&"

# characters preceded by space in regularized form that don't have the
# space in pressed form
open_rx_set = "[][{}?!%s]" % token_rx_subset

# characters followed by space in regularized form that don't have the
# space in pressed form; '.' isn't included because it's valid in
# string constants (inside name)
close_rx_set = "[][{},;%s]" % token_rx_subset

open_rx = re.compile(" (%s)" % open_rx_set)

pressed_open_rx = re.compile("(?<![(! %s])(%s)" % (token_rx_subset, open_rx_set))

close_rx = re.compile("(%s) " % close_rx_set)

pressed_close_rx = re.compile("(%s)(?![ %s])" % (close_rx_set, token_rx_subset))

# doesn't include values because wikidata might fail on values
# not preceded by a space, e.g. on https://query.wikidata.org/sparql?format=json&query=select%3Fw%3Fl%3Fp%3Fo{%3Fw+wdt:P27+wd:Q213%3Brdfs:label%3Fl%3Bwdt:P39%3Fp.%3Fw+wdt:P106%3Fo.filter(lang(%3Fl)%3D"cs"%26%26contains(lcase(%3Fl)%2C"jaroslav+%C4%8Dech"))%3Fw+wdt:P106+wd:Q81096.values%3Fo{wd:Q82955}}
word_rx = re.compile(" (filter|optional|\"cs\")")

begin_rx = re.compile("^([^{]+{) ")

clause_rx = re.compile(" (} union {) ")

end_rx = re.compile("(})$")

def normalize_url_param(path):
    # Must be a subset of safe chars in normalize_url_component. OTOH
    # this function quotes a single parameter, so the chars cannot
    # contain '?', '&' or ';'.
    q = parse.quote(path, safe="/()[]{}:!|\"")
    return space_rx.sub('+', q)

def create_query_url(query):
    mq = whitespace_rx.sub(" ", query.strip())
    half = open_rx.sub("\\1", mq)
    shorter = close_rx.sub("\\1", half)
    shortest = word_rx.sub("\\1", shorter)
    return query_url_head + normalize_url_param(shortest)

def reflate(q):
    half = pressed_close_rx.sub("\\1 ", q)
    reg = pressed_open_rx.sub(" \\1", half)
    two = begin_rx.sub("\\1\n  ", reg.strip())
    multi = clause_rx.sub("\n\\1\n", two.strip())
    return end_rx.sub("\n\\1", multi)

def extract_query(qurl):
    uo = parse.urlparse(qurl)
    params = parse.parse_qsl(uo.query)
    for p in params:
        if p[0] == 'query':
            return reflate(p[1])

    return None

def print_query(qurl):
    q = extract_query(qurl)
    if (q):
        print(q)

    print("")
