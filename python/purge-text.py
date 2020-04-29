#!/usr/bin/python3

import os
import shutil
import sys
from common import get_loose_path, get_parent_directory, make_connection, schema
from cursor_wrapper import CursorWrapper
from url_heads import short_town_url_head

def get_alt_repr_dir(alt_repr):
    assert alt_repr

    tmp_dir = os.path.join(get_parent_directory(), "tmp")

    if schema:
        tmp_dir = os.path.join(tmp_dir, schema)

    return os.path.join(tmp_dir, alt_repr)


class Purger(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        self.shrunk = set() # volume IDs
        self.doomed = set() # URL IDs

    def run(self):
        self.cur.execute("""select url, id
from field
where url ~ '^%s[^#]+#[0-9]+$'
order by url""" % short_town_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.purge(*row)

    def purge(self, url, url_id):
        print("purging %s..." % (url,), file=sys.stderr)

        loose_path = get_loose_path(url_id)
        self.ensure_removed(loose_path)

        self.cur.execute("""delete from redirect
where from_id=%s or to_id=%s""", (url_id, url_id))

        self.cur.execute("""delete from field
where id=%s""", (url_id,))

    @staticmethod
    def ensure_removed(path):
        if os.path.exists(path):
            os.remove(path)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            purger = Purger(cur)
            purger.run()

    ard = get_alt_repr_dir('majka')
    if os.path.exists(ard):
        shutil.rmtree(ard)


if __name__ == "__main__":
    main()
