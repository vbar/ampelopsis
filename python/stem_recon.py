#!/usr/bin/python3

# requires downloaded data extended by running text-stemmer.py

import os
import re
import sys
from common import get_loose_path, make_connection
from token_util import url_rx

fragment_rx = re.compile("^[^#]+#([0-9]+)$")


class StemException(Exception):
    pass


def get_fragment(surl):
    m = fragment_rx.match(surl)
    if not m:
        raise Exception("not a sentence URL: " + surl)

    return int(m.group(1))


def get_stem(tail):
    items = tail.split(':')
    l = len(items)
    if (not l) or (l % 2):
        raise StemException("odd items in " + tail)

    stem2freq = {}
    for i in range(0, l, 2):
        stem = items[i]
        freq = stem2freq.get(stem, 0)
        stem2freq[stem] = freq + 1

    for stem, req in sorted(stem2freq.items(), key=lambda p: (-1 * p[1], len(p[0]))):
        return stem


def reconstitute_line(url_id):
    sentence = []
    loose_path = get_loose_path(url_id, alt_repre='majka')
    try:
        with open(loose_path, "r", encoding ='utf-8') as f:
            for raw in f:
                ln = raw.strip()
                if url_rx.match(ln):
                    sentence.append(ln)
                elif ln:
                    wln = ln.split(':', 1)
                    l = len(wln)
                    if not l or not wln[0]:
                        raise Exception(loose_path + ": no source word in " + ln)

                    if (l < 2) or not wln[1]:
                        sentence.append(ln)
                    else:
                        sentence.append(get_stem(wln[1]))

        return " ".join(sentence)
    except StemException as ex:
        raise Exception(loose_path + ": " + str(ex))


def reconstitute(cur, url):
    mask = url + '#%'
    cur.execute("""select url, id
from field
where url like %s
order by url""", (mask,))
    rows = cur.fetchall()
    frag2ln = {}
    for surl, url_id in rows:
        frag2ln[get_fragment(surl)] = reconstitute_line(url_id)

    rect = []
    for frag, ln in sorted(frag2ln.items()):
        rect.append(ln)

    return "\n".join(rect)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            for url in sys.argv[1:]:
                rect = reconstitute(cur, url)
                print(rect, "\n")


if __name__ == "__main__":
    main()
