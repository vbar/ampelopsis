#!/usr/bin/python3

import json
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper
from leaf_merge import LeafMerger
from volume_holder import VolumeHolder

class Dumper(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.leaf_merger = LeafMerger(cur)

    def dump(self, url, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if reader:
            try:
                self.do_dump(url, reader)
            finally:
                reader.close()

    def do_dump(self, url, fp):
        m = self.leaf_merger.person_url_rx.match(url)
        if not m:
            print("skipping non-person URL " + url, file=sys.stderr)
            return

        person_id = m.group('id')

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))
        self.leaf_merger.merge(person_id, doc)
        print(json.dumps(doc))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            dumper = Dumper(cur)
            try:
                for url in sys.argv[1:]:
                    cur.execute("""select id, checkd
from field
where url=%s""", (url,))
                    row = cur.fetchone()
                    if not row:
                        print(url + " not found", file=sys.stderr)
                    else:
                        if row[1] is None:
                            print(url + " not downloaded", file=sys.stderr)
                        else:
                            dumper.dump(url, row[0])
            finally:
                dumper.close()


if __name__ == "__main__":
    main()
