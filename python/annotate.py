#!/usr/bin/python3

import hashlib
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class Extender(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

    def extend(self, url, url_id):
        print("hashing " + url + "...", file=sys.stderr)
        
        volume_id = self.get_volume_id(url_id)
        content_type = self.get_content_type(url_id, volume_id)
        
        f = self.open_headers(url_id, volume_id)
        if f is not None:
            sz = self.get_body_size(url_id, volume_id)
            self.hash_content(url_id, content_type, f, sz)
        else:
            self.cur.execute("""insert into extra(url_id, content_type, has_body)
values(%s, %s, false)""", (url_id, content_type))
                    
    def hash_content(self, url_id, content_type, f, sz):
        h = hashlib.sha1()
        for ln in f:
            h.update(ln)
            
        self.cur.execute("""insert into extra(url_id, content_type, hash, siz)
values(%s, %s, %s, %s)""", (url_id, content_type, h.hexdigest(), sz))

    def get_content_type(self, url_id, volume_id):
        content_type = None
        f = self.open_headers(url_id, volume_id)
        if f is not None:
            try:
                for ln in f:
                    header_line = ln.decode('iso-8859-1')
                    line_list = header_line.split(':', 1)
                    if len(line_list) == 2:
                        name, value = line_list
                        name = name.strip()
                        name = name.lower()
                        value = value.strip()

                        if name == 'content-type':
                            content_type = value
            finally:
                f.close()
                
        return content_type
    

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            extender = Extender(cur)
            try:
                cur.execute("""select url, id
from field
left join extra on field.id=extra.url_id
where checkd is not null and has_body is null
order by url""")
                rows = cur.fetchall()
                for row in rows:
                    extender.extend(*row) 
            finally:
                extender.close()
            
if __name__ == "__main__":
    main()
        
