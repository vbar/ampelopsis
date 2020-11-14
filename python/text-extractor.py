#!/usr/bin/python3

import os
import re
import sys
from urllib.parse import urlparse
from common import get_loose_path, make_connection
from show_case import ShowCase
from token_util import url_rx

invalid_bytes_rx = re.compile(b'[^\x09\x0A\x0D\x20-\xff]')

replacement_rx = re.compile('\uFFFD')

def morpho_tokenize(txt):
    seq = txt.split()
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
        url_id = self.ensure_url_id(surl)

        txt = self.save_clean_text(url, et['text'])
        fname = get_loose_path(url_id)
        lst = morpho_tokenize(txt)
        if len(lst):
            with open(fname, 'w', encoding ='utf-8') as f:
                lw = lst.pop()
                for w in lst:
                    f.write(w + " ")

                f.write(lw + "\n")
        elif os.path.exists(fname):
            os.remove(fname)

    def ensure_url_id(self, url):
        # simpler than download updates because it isn't safe for
        # parallel instances
        self.cur.execute("""insert into field(url, checkd, parsed)
values(%s, localtimestamp, localtimestamp)
on conflict(url) do nothing
returning id""", (url,))
        row = self.cur.fetchone()
        if row:
            return row[0]
        else:
            return self.get_url_id(url)

    def save_clean_text(self, url, raw):
        # lxml/libxml2 is touchier about invalid Unicode characters than
        # core Python: remove them before producing XML from them. Code
        # below might seem redundant, but nothing simpler worked to remove
        # Ctrl-C... The REPLACEMENT CHARACTER is removed so that documents
        # containing nothing but invalid characters can be (easily)
        # recognized as empty.
        b = raw.encode('utf-8')
        c = invalid_bytes_rx.sub(b'', b)
        txt = c.decode('utf-8', 'ignore')
        stripped = replacement_rx.sub('', txt)

        url_id = self.ensure_url_id(url)

        fname = get_loose_path(url_id, alt_repre='morphodita')
        if len(stripped):
            with open(fname, 'w', encoding ='utf-8') as f:
                f.write(stripped)
        elif os.path.exists(fname):
            os.remove(fname)

        return stripped


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
