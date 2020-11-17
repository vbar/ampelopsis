#!/usr/bin/python3

import sys
from common import get_option

class LetterAnalyzer:
    def __init__(self):
        self.ngram_size = int(get_option("letter_ngram_size", "2"))
        if self.ngram_size < 1:
            raise Exception("letter_ngram_size must be positive")

    def __call__(self, rect):
        sentences = rect.split("\n")
        terms = []
        for s in sentences:
            terms.extend(self.split_sentence(s))

        return terms

    def split_sentence(self, s):
        l = len(s)
        terms = []
        for i in range(min(l - 1, self.ngram_size)):
            terms.append(s[:i+1])

        i = self.ngram_size + 1
        while i <= l:
            terms.append(s[i-self.ngram_size:i])
            i += 1

        i = l - self.ngram_size + 1
        while i < l:
            terms.append(s[i:])
            i += 1

        return terms


def main():
    an = LetterAnalyzer()
    ln = " ".join(sys.argv[1:])
    terms = an.split_sentence(ln)
    for t in terms:
        print(t)


if __name__ == "__main__":
    main()
