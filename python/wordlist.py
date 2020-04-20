#!/usr/bin/python3

import os
import sys
from common import get_parent_directory, make_connection
from show_case import ShowCase
from token_util import tokenize

class WordList(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.doc_count = 0
        self.word2count = {}

    def load_item(self, et):
        self.doc_count += 1
        lst = tokenize(et['text'])
        bag = set(lst)
        for w in bag:
            cnt = self.word2count.get(w, 0)
            self.word2count[w] = cnt + 1

    def dump(self):
        print("%d documents" % self.doc_count, file=sys.stderr)
        if self.doc_count:
            cache_dir = os.path.join(get_parent_directory(), "cache")
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)

            word_list_file = os.path.join(cache_dir, "wordlist.txt")
            with open(word_list_file, 'w') as f:
                for w, c in sorted(self.word2count.items(), key=lambda p: (-1 * p[1], p[0])):
                    print(w, "\t", c / self.doc_count, file=f)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            wl = WordList(cur)
            wl.run()
            wl.dump()


if __name__ == "__main__":
    main()
