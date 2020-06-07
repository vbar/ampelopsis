from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from common import get_netloc

def get_next_query(prev_query, cursor):
    prev_dict = parse_qs(prev_query)
    next_seq = []
    for k, v in sorted(prev_dict.items()):
        v = v[0] if k != 'max_position' else cursor
        next_seq.append((k, v))

    return urlencode(next_seq)


def get_next_url(prev_url, cursor):
    if not cursor:
        return None

    pr = urlparse(prev_url)
    next_query = get_next_query(pr.query, cursor)
    next_pr = (pr.scheme, get_netloc(pr), pr.path, pr.params, next_query, '')
    return urlunparse(next_pr)
