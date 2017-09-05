#!/usr/bin/python3

import hashlib
import sys
import zipfile
from common import get_volume_path, make_connection

class Extender:
    def __init__(self, cur):
        self.cur = cur
        self.zp = None
        self.volume_id = None

    def extend(self, url, url_id, volume_id, member_id):
        print("hashing " + url + "...", file=sys.stderr)
        
        if volume_id != self.volume_id:
            self.change_volume(volume_id)

        try:
            info = self.zp.getinfo(str(member_id))
        except KeyError:
            info = None

        if info is not None:
            self.hash_content(url_id, info)
        else:
            self.cur.execute("""insert into extra(url_id, has_body) values(%s, false)""", (url_id,))
                    
    def hash_content(self, url_id, info):
        h = hashlib.sha1()
        h.update(self.zp.read(info))
        self.cur.execute("""insert into extra(url_id, hash, siz)
values(%s, %s, %s)""", (url_id, h.hexdigest(), info.file_size))
            
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
    with make_connection() as conn:
        with conn.cursor() as cur:
            extender = Extender(cur)
            try:
                cur.execute("""select url, content.url_id, volume_id, member_id
from field
join content on field.id=content.url_id
join directory on directory.id=volume_id
left join extra on field.id=extra.url_id
where checkd is not null and written is not null and has_body is null
order by volume_id, url""")
                rows = cur.fetchall()
                for row in rows:
                    extender.extend(*row)                    
            finally:
                extender.close()
            
if __name__ == "__main__":
    main()
        
