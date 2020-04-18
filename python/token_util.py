import re

url_rx = re.compile('https?://')

split_rx = re.compile('\\W+')

def tokenize(raw, inclinks=True):
    """Twitter-specific tokenization."""
    lst = []
    for w in raw.split():
        if w:
            if (w[0] in ('@', '#')) or url_rx.match(w):
                if inclinks:
                    lst.append(w)
            else:
                lst.extend((sw for sw in split_rx.split(w) if sw))

    return [w.lower() for w in lst]


def tokenize_persons(raw):
    """Tokenization restricted to @-links."""
    lst = []
    for w in raw.split():
        if w and (w[0] == '@'):
            lst.append(w)

    return [w.lower() for w in lst]


def retokenize(s):
    return s.split()
