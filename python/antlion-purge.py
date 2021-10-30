#!/usr/bin/python3

import csv
import json
import os
import re
import sys
from urllib.parse import urlparse
from common import get_loose_path, get_option, get_parent_directory, make_connection
from cursor_wrapper import CursorWrapper
from json_frame import JsonFrame
from purge import Purger
from url_heads import hamlet_url_head

status_rx = re.compile("^/([-\\w]+)/status/")

class PitPurger(JsonFrame):
    def __init__(self, cur, maw_file):
        JsonFrame.__init__(self, cur)

        maw = set()
        with open(maw_file, newline='') as f:
            reader = csv.DictReader(f, delimiter=",")
            for row in reader:
                pr = urlparse(row['url'])
                name = pr.path[1:]
                maw.add(name)

        self.doomed = {} # str URL -> int URL ID
        self.cur.execute("""select f1.url, f2.url, from_id, to_id
from redirect
join field f1 on from_id=f1.id
join field f2 on to_id=f2.id
order by from_id, to_id""")
        rows = self.cur.fetchall()
        for source_url, target_url, source_id, target_id in rows:
            target_name = self.get_account_name(target_url)
            if target_name in maw:
                self.doomed[source_url] = source_id
                self.doomed[target_url] = target_id

    def rewrite_pages(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s'
and checkd is not null
and url_id is null
order by url""" % hamlet_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.rewrite_page(*row)

    def purge_doomed(self):
        print("purging %d URLs" % len(self.doomed.keys()), file=sys.stderr)

        purger = Purger(self.cur)
        for url, url_id in sorted(self.doomed.items()):
            print("purging %s..." % url, file=sys.stderr)
            purger.purge_fast(url_id)

        purger.purge_rest()

    def rewrite_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)

        raw_items = doc.get('results')
        filtered_items = []
        for et in raw_items:
            if et['url'] not in self.doomed:
                filtered_items.append(et)

        if len(raw_items) == len(filtered_items):
            return

        doc['results'] = filtered_items
        with open(get_loose_path(url_id), 'w') as f:
            json.dump(doc, f)

    @staticmethod
    def get_account_name(url):
        pr = urlparse(url)
        m = status_rx.match(pr.path)
        if not m:
            return None

        town_name = m.group(1)
        return town_name.lower()


def main():
    cache_dir = os.path.join(get_parent_directory(), "cache")
    if os.path.exists(cache_dir):
        maw_file = os.path.join(cache_dir, "maw.csv")
        if not os.path.exists(maw_file):
            raise Exception("no maw file")

    conn = make_connection()
    try:
        with conn.cursor() as cur:
            pp = PitPurger(cur, maw_file)
            pp.rewrite_pages()
            pp.purge_doomed()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
