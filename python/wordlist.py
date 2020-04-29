#!/usr/bin/python3

import os
import sys
from common import get_option, get_parent_directory, make_connection
from opt_util import get_cache_path
from show_case import ShowCase
from stem_recon import reconstitute
from token_util import retokenize, tokenize

class WordList(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.doc_count = 0
        self.word2count = {}
        self.tokenize_item = self.tokenize_rect if get_option("use_stemmed", True) else self.tokenize_text

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
                    print(w, "\t", c / self.doc_count, file=f)

    def tokenize_text(self, et):
        return tokenize(et['text'], True)

    def tokenize_rect(self, et):
        rect = reconstitute(self.cur, et['url'])
        return retokenize(rect)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            wl = WordList(cur)
            wl.run()
            wl.dump()


if __name__ == "__main__":
    main()
