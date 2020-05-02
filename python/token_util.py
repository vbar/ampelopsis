import re

url_rx = re.compile('https?://')

split_rx = re.compile('\\W+')

link_rx = re.compile("[@#]+([^@#]*)")

link_split_rx = re.compile('[:,;.?!]')

num_rx = re.compile('[0-9]')

def tokenize_list(seq, inclinks):
    lst = []
    for w in seq:
        found_link = False
        m = link_rx.match(w)
        while m:
            found_link = True
            link = w[0] + m.group(1)

            # must remove colons - external stemmer doesn't handle
            # them (and they probably aren't part of links
            # anyway); the other punctuation is removed as a bonus
            sseq = [sw for sw in link_split_rx.split(link)]
            head = sseq.pop()
            if inclinks:
                lst.append(head)

            lst.extend(tokenize_list(sseq, inclinks))

            w = w[len(link):]
            m = link_rx.match(w)

        if not found_link:
            if url_rx.match(w):
                # stemming is special-cased to handle colons in this case
                if inclinks:
                    lst.append(w)
            elif w:
                lst.extend((sw for sw in split_rx.split(w) if sw and not num_rx.search(w)))

    return lst


def tokenize(raw, inclinks=True):
    """Twitter-specific tokenization."""
    lst = tokenize_list(raw.split(), inclinks)
    return [w.lower() for w in lst]


def retokenize(s):
    return s.split()
