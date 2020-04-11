#!/usr/bin/python3

import sys
from common import make_connection
from lang import init_lang_recog
from show_case import ShowCase
from token_util import tokenize

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.lang_recog = init_lang_recog()
        self.lang2freq = {}

    def load_item(self, et):
        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if not lng:
            lng = 'other'

        cnt = self.lang2freq.get(lng, 0)
        self.lang2freq[lng] = cnt + 1

    def dump(self):
        for lng, frq in sorted(self.lang2freq.items(), key=lambda p: (-1 * p[1], p[0])):
            print(lng, "\t", frq)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            processor.dump()


if __name__ == "__main__":
    main()
