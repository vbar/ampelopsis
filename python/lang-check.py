#!/usr/bin/python3

import sys
from common import make_connection
from lang import init_lang_dict
from show_case import ShowCase
from token_util import tokenize

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.lang_dict = init_lang_dict()
        self.lang2freq = {}

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            lst = tokenize(et['text'])
            lng = self.lang_dict.check(set(lst))
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
