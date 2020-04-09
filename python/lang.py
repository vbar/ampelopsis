#!/usr/bin/python3

import re
import sys

class MultiDict:
    def __init__(self):
        self.dict = {}
        self.bit2lang = {}
        self.cur_bit = 1

    def fill(self, lang, fname, enc):
        assert lang
        assert fname

        if enc is None:
            enc = 'utf8'

        bit = self.cur_bit
        assert bit
        self.cur_bit *= 2

        self.bit2lang[bit] = lang
        with open(fname, "rb") as f:
            for ln in f:
                b = ln.rstrip()
                w = b.decode(enc)
                a = w.split("/")
                if len(a) == 2:
                    w = a[0]

                if (len(w) > 2) and not w[0].isupper(): # skip names
                    bits = self.dict.get(w, 0)
                    self.dict[w.lower()] = bits | bit

    def prune(self):
        keys = [ k for k in self.dict.keys() ]
        single = set(self.bit2lang.keys())
        for k in keys:
            if self.dict[k] not in single:
                del self.dict[k]

        self.cur_bit = 0

    def check(self, bag):
        assert not self.cur_bit # multi-language words must be pruned before checking

        bit2count = {}
        for w in bag:
            b = self.dict.get(w)
            if b:
                c = bit2count.get(b, 0)
                bit2count[b] = c + 1

        alts = sorted(bit2count.keys(), key=lambda b: -1 * bit2count[b])
        if not len(alts):
            return None

        cand = alts.pop(0)
        if not len(alts):
            return self.bit2lang[cand]

        lead = bit2count[cand]
        for a in alts:
            lead -= bit2count[a]

        return self.bit2lang[cand] if lead >= 10 else None


def main():
    md = MultiDict()
    md.fill('cs_CZ', "/usr/share/hunspell/cs_CZ.dic", 'latin2')
    md.fill('en_US', "/usr/share/hunspell/en_US.dic", None)
    md.prune()

    for fname in sys.argv[1:]:
        with open(fname) as f:
            body = f.read()
            bag = set(re.split('\\W+', body))
            print(bag)
            print(md.check(bag))

if __name__ == "__main__":
    main()
