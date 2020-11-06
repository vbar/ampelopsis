#!/usr/bin/python3

import re
import sys
from urllib.parse import urlparse
from common import get_loose_path, make_connection
from show_case import ShowCase
from token_util import url_rx

def morpho_tokenize(raw):
    seq = raw.split()
    lst = []
    for w in seq:
        if url_rx.match(w):
            lst.append(w) # MorphoDiTa tokenization seems to recognize URLs
        else:
            if w[0] in ('@', '#'):
                lst.append(w[0])
            else:
                lst.append(w)

    return lst


class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)

    def load_item(self, et):
        url = et['url']
        pr = urlparse(url)
        if pr.fragment:
            raise Exception(url + " already has fragment")

        surl = url + "#plain"
        # simpler than download updates because it isn't safe for
        # parallel instances
        self.cur.execute("""insert into field(url, checkd, parsed)
values(%s, localtimestamp, localtimestamp)
on conflict(url) do nothing
returning id""", (surl,))
        row = self.cur.fetchone()
        if row:
            url_id = row[0]
        else:
            print(surl + " already exists", file=sys.stdout)
            url_id = self.get_url_id(surl)

        lst = morpho_tokenize(et['text'])
        if len(lst):
            fname = get_loose_path(url_id)
            with open(fname, 'w', encoding ='utf-8') as f:
                lw = lst.pop()
                for w in lst:
                    f.write(w + " ")

                f.write(lw + "\n")


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
