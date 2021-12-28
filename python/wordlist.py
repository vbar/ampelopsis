#!/usr/bin/python3

import os
import sys
from common import get_option, get_parent_directory, make_connection
from majka_tap import MajkaTap
from morphodita_tap import MorphoditaTap
from opt_util import get_cache_path
from show_case import ShowCase
from token_util import retokenize, tokenize

class WordList(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.doc_count = 0
        self.word2count = {}
        stemmer = get_option("active_stemmer", "morphodita")
        if stemmer:
            if stemmer == "majka":
                self.tap = MajkaTap(self.cur)
            elif stemmer == "morphodita":
                self.tap = MorphoditaTap(self.cur)
            else:
                raise Exception("unknown stemmer: " + stemmer)

            self.tokenize_item = self.tokenize_rect
        else:
            self.tokenize_item = self.tokenize_text

        self.cond_case = self.lower_case if get_option("lowercase", "1") else self.preserve_case

    def load_item(self, et):
        self.doc_count += 1
        lst = self.tokenize_item(et)
        bag = set(lst)
        for w in bag:
            cnt = self.word2count.get(w, 0)
            self.word2count[w] = cnt + 1

    def dump(self):
        print("%d documents" % self.doc_count, file=sys.stderr)
        if self.doc_count:
            word_list_file = get_cache_path("wordlist.txt", mkdir=True)
            with open(word_list_file, 'w') as f:
                for w, c in sorted(self.word2count.items(), key=lambda p: (-1 * p[1], p[0])):
                    print("%s\t%s" % (w, c / self.doc_count), file=f)

    def tokenize_text(self, et):
        return tokenize(self.cond_case(et['text']), True)

    def tokenize_rect(self, et):
        rect = self.tap.reconstitute(et['url'])
        return retokenize(self.cond_case(rect))

    def preserve_case(self, txt):
        return txt

    def lower_case(self, txt):
        return txt.lower()


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            wl = WordList(cur)
            wl.run()
            wl.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
