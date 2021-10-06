#!/usr/bin/python3

import sys
import zipfile
from common import make_connection
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class Dumper(VolumeHolder, CursorWrapper):
    def __init__(self, cur, dump_header):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

        self.dump_header = dump_header

    def dump(self, url, url_id):
        volume_id = self.get_volume_id(url_id)

        if not self.dump_header:
            reader = self.open_page(url_id, volume_id)
        else:
            reader = self.open_headers(url_id, volume_id)

        if reader:
            try:
                for ln in reader:
                    sys.stdout.buffer.write(ln)
            finally:
                reader.close()

def main():
    dump_header = False
    if (len(sys.argv) > 1) and (sys.argv[1] == '-H'):
        dump_header = True
        del sys.argv[1]

    conn = make_connection()
    try:
        with conn.cursor() as cur:
            dumper = Dumper(cur, dump_header)
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
    finally:
        conn.close()


if __name__ == "__main__":
    main()
