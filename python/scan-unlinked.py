#!/usr/bin/python3

# requires speech URLs

import csv
import sys
from common import make_connection
from json_frame import JsonFrame

class Scanner(JsonFrame):
    def __init__(self, cur):
        JsonFrame.__init__(self, cur)
        self.unlinked = set() # of name, position pairs

    def run(self):
        self.cur.execute("""select url, id
from field
where url like 'http://localhost/%'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.check(*row)

    def dump(self, writer):
        writer.writerow(['name', 'position'])
        for pair in sorted(self.unlinked):
            writer.writerow(list(pair))

    def check(self, speech_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(speech_url + " not found on disk", file=sys.stderr)
            return

        print("checking %s..." % (speech_url,), file=sys.stderr)
        if 'speaker_url' not in doc:
            self.unlinked.add((doc.get('speaker_name', ''), doc.get('speaker_position', '')))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur)
            scanner.run()
            writer = csv.writer(sys.stdout, delimiter=",")
            scanner.dump(writer)


if __name__ == "__main__":
    main()
