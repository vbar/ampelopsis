#!/usr/bin/python3

import sys
from common import get_option
from token_util import url_rx

class Analyzer:
    def __init__(self, stop_words):
        self.stop_words = frozenset(stop_words)
        self.ngram_min = int(get_option("vectorizer_ngram_min", "1"))
        if self.ngram_min < 1:
            raise Exception("vectorizer_ngram_min must be positive")

        self.ngram_max = int(get_option("vectorizer_ngram_max", "1"))
        if self.ngram_max < self.ngram_min:
            raise Exception("vectorizer_ngram_max must be at least vectorizer_ngram_min")

    def __call__(self, rect):
        sentences = rect.split("\n")
        terms = []
        for s in sentences:
            terms.extend(self.split_sentence(s))

        return terms

    def split_sentence(self, s):
        head = []
        cur_frag = []
        tail = []
        for w in s.split(" "):
            if w and w not in self.stop_words:
                if (w[0] in ('#', '@')) or url_rx.match(w):
                    head.append(w)
                    if len(cur_frag):
                        tail.extend(self.compose_ngrams(cur_frag))
                        cur_frag = []
                else:
                    cur_frag.append(w)

        head.extend(tail)
        return head

    def compose_ngrams(self, words):
        terms = []
        l = len(words)
        for i in range(0, l):
            k = min(i + self.ngram_max, l)
            for j in range(i + self.ngram_min, k + 1):
                slc = words[i:j]
                terms.append(" ".join(slc))

        return terms


def main():
    an = Analyzer([])
    ln = " ".join(sys.argv[1:])
    terms = an.split_sentence(ln)
    for t in terms:
        print(t)


if __name__ == "__main__":
    main()
