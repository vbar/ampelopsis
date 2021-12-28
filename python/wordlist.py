#!/usr/bin/python3

import os
import sys
from common import get_option, get_parent_directory, make_connection
from morphodita_conv import make_tagger, simplify_fulltext
from opt_util import get_cache_path
from show_case import ShowCase
from token_util import tokenize

class WordList(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.tagger = make_tagger()
        self.doc_count = 0
        self.word2count = {}

    def load_item(self, doc):
        self.doc_count += 1

        txt = doc.get('text')
        if not txt:
            return

        bag = set()
        lst = tokenize(txt)
        simple_text = simplify_fulltext(self.tagger, txt)
        for raw_word in simple_text.split():
            cased = raw_word[:-1] if raw_word.endswith('.') else raw_word
            bag.add(cased.lower())

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
