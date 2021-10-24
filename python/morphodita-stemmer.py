#!/usr/bin/python3

# requires run_tagger executable on path, its datafile in cache
# directory and downloaded data extended by running text-model.py

import os
import shutil
import subprocess
import sys
from common import get_loose_path, get_mandatory_option, get_option, make_connection
from cursor_wrapper import CursorWrapper
from opt_util import get_cache_path

class Processor(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        self.plain_repre = get_mandatory_option("storage_alternative")
        self.morpho_repre = get_option("morphodita_output", "morpho")

        stemmer = shutil.which('run_tagger')
        if not stemmer:
            raise Exception("run_tagger not found")

        stemmer_data = get_cache_path(get_option("morphodita_tagger_file", "czech-morfflex-pdt-161115.tagger"))
        if not os.path.isfile(stemmer_data):
            raise Exception("required file %s not found" % stemmer_data)

        self.batch_size = int(get_option("morphodita_batch_size", "100"))
        self.args_head = [ stemmer, stemmer_data ]
        self.args_tail = []

    def run(self):
        self.cur.execute("""select url, id
from field
where url like 'https://apps.odok.cz/%'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.stem_doc(*row)

        self.flush()

    def stem_doc(self, url, url_id):
        src_path = get_loose_path(url_id, alt_repre=self.plain_repre)
        if not os.path.exists(src_path):
            return

        print("stemming %s..." % (url,), file=sys.stderr)
        dest_path = get_loose_path(url_id, alt_repre=self.morpho_repre)
        self.args_tail.append(src_path + ':' + dest_path)
        if len(self.args_tail) >= self.batch_size:
            self.flush()

    def flush(self):
        if not len(self.args_tail):
            return

        args = self.args_head[:]
        args.extend(self.args_tail)
        subprocess.run(args, check=True)
        self.args_tail = []


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
