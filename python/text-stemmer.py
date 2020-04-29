#!/usr/bin/python3

# requires majka executable on path, its datafile in cache directory
# and downloaded data extended by running text-separator.py

import os
import shutil
import subprocess
import sys
from common import get_loose_path, make_connection
from cursor_wrapper import CursorWrapper
from opt_util import get_cache_path
from url_heads import short_town_url_head

class Processor(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)

        stemmer = shutil.which('majka')
        if not stemmer:
            raise Exception("majka not found")

        stemmer_data = get_cache_path("majka.w-lt")
        if not os.path.isfile(stemmer_data):
            raise Exception("required file %s not found" % self.stemmer_data)

        self.args = [ stemmer, '-f', stemmer_data, '-p' ]

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s[^#]+#[0-9]+$'
order by url""" % short_town_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.stem_sentence(*row)

    def stem_sentence(self, sentence_url, url_id):
        if self.get_volume_id(url_id):
            print("not stemming archived %s" % (sentence_url,), file=sys.stderr)
            return

        print("stemming %s..." % (sentence_url,), file=sys.stderr)

        src_path = get_loose_path(url_id)
        with open(src_path, "rb") as infile:
            dest_path = get_loose_path(url_id, alt_repre='majka')
            with open(dest_path, "wb") as outfile:
                subprocess.run(self.args, stdin=infile, stdout=outfile, check=True)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()


if __name__ == "__main__":
    main()
