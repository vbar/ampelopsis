import collections
import re
from urlize import whitespace_rx

Personage = collections.namedtuple('Personage', 'presentation_name query_name birth_year')

title_rx = re.compile("^([^\\(\\)]+)\\(\\*([0-9]{4})(?:\\)| - )")

# we could include single quote, but there probably aren't any Czech
# politicians named O'Something...
name_char_rx = re.compile("[^\\w ./-]")


def normalize_name(raw):
    name = name_char_rx.sub("", raw)
    return name.lower()


def parse_personage(raw_title):
    m = title_rx.match(raw_title.strip())
    if not m:
        return None

    present_name = m.group(1)
    year = int(m.group(2))

    segments = present_name.split(',', 2)
    names = segments[0].split()
    l = len(names)
    assert l
    if l == 1:
        qn = normalize_name(names[0])
    else:
        qn = "%s %s" % tuple(normalize_name(names[n]) for n in (-2, -1))

    if not qn.strip():
        qn = None

    canon_name = whitespace_rx.sub(" ", present_name.strip())
    return Personage(presentation_name=canon_name.rstrip(','), query_name=qn, birth_year=year)
