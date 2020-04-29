#!/usr/bin/python3

from separator import separate
import sys
from urllib.parse import urlparse
from common import get_loose_path, make_connection
from show_case import ShowCase
from token_util import tokenize

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)

    def load_item(self, et):
        url = et['url']
        pr = urlparse(url)
        if pr.fragment:
            raise Exception(url + " already has fragment")

        sentences = separate(et['text'])
        idx = 0
        for s in sentences:
            if self.store_sentence(url, idx, s):
                idx += 1

    def store_sentence(self, parent_url, idx, raw):
        lst = tokenize(raw)
        if not len(lst):
            return False

        surl = "%s#%d" % (parent_url, idx)
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

        fname = get_loose_path(url_id)
        with open(fname, 'w', encoding ='utf-8') as f:
            for w in lst:
                f.write(w + "\n")

        return True


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
