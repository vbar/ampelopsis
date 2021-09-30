#!/usr/bin/python3

import sys
from common import make_connection
from show_case import ShowCase

class Extender(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.pool = {} # str text -> set of str URL

    def load_item(self, et):
        url = et['url']
        txt = et['text']
        url_set = self.pool.get(txt)
        if url_set is None:
            self.pool[txt] = set((url,))
        else:
            url_set.add(url)

    def process(self):
        for txt, url_set in self.pool.items():
            if len(url_set) > 1:
                url_list = sorted(url_set, key=lambda u: (len(u), u))
                target_url = url_list.pop(0)
                print("redirecting to %s..." % target_url)
                target_url_id = self.ensure_url_id(target_url)
                for source_url in url_list:
                    source_url_id = self.ensure_url_id(source_url)
                    self.add_redirect(source_url_id, target_url_id)

    def add_redirect(self, source_url_id, target_url_id):
        if source_url_id != target_url_id:
            self.cur.execute("""insert into redirect(from_id, to_id)
values(%s, %s)
on conflict do nothing""", (source_url_id, target_url_id))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            ext = Extender(cur)
            ext.run()
            ext.process()


if __name__ == "__main__":
    main()
