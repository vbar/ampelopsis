import re

url_rx = re.compile('https?://')

split_rx = re.compile('\\W+')

num_rx = re.compile('[0-9]')

def tokenize(raw, inclinks=True):
    """Twitter-specific tokenization."""
    lst = []
    for w in raw.split():
        if w:
            if w[0] in ('@', '#'):
                # remove colons - external stemmer doesn't handle them
                # (and they probably aren't part of links anyway)
                if inclinks:
                    lst.extend((sw for sw in w.split(':') if sw))
            elif url_rx.match(w):
                # stemming is special-cased to handle colons in this case
                if inclinks:
                    lst.append(w)
            else:
                lst.extend((sw for sw in split_rx.split(w) if sw and not num_rx.search(w)))

    return [w.lower() for w in lst]


def retokenize(s):
    return s.split()
