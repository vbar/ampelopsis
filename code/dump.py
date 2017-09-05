#!/usr/bin/python3

import sys
import zipfile
from common import get_volume_path, make_connection

class Dumper:
    def __init__(self, cur, dump_header):
        self.cur = cur
        self.dump_header = dump_header
        self.zp = None
        self.volume_id = None

    def dump(self, url, volume_id, member_id):
        if volume_id != self.volume_id:
            self.change_volume(volume_id)

        try:
            info = self.zp.getinfo(self.format_member_name(member_id))
        except KeyError:
            print(url + " body not found", file=sys.stderr)
            return

        with self.zp.open(info) as reader:
            for ln in reader:
                sys.stdout.buffer.write(ln)
            
    def format_member_name(self, member_id):
        name = str(member_id)
        if self.dump_header:
            name += 'h'

        return name
    
    def change_volume(self, volume_id):
        if self.zp is not None:
            self.zp.close()

        archive_path = get_volume_path(volume_id)
        self.zp = zipfile.ZipFile(archive_path)
        self.volume_id = volume_id
        
    def close(self):
        if self.zp is not None:
            self.zp.close()
            self.zp = None

def main():
    dump_header = False
    if (len(sys.argv) > 1) and (sys.argv[1] == '-H'):
        dump_header = True
        del sys.argv[1]
        
    with make_connection() as conn:
        with conn.cursor() as cur:
            dumper = Dumper(cur, dump_header)
            try:
                for url in sys.argv[1:]:
                    cur.execute("""select volume_id, member_id
from field
join content on field.id=url_id
join directory on directory.id=volume_id
where checkd is not null and written is not null and url=%s""", (url,))
                    row = cur.fetchone()
                    if not row:
                        print(url + " not found", file=sys.stderr)
                    else:
                        dumper.dump(url, *row)
            finally:
                dumper.close()
            
if __name__ == "__main__":
    main()
        
