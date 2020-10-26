#!/usr/bin/python3

import os
import time
import zipfile
from common import get_loose_path, get_option, get_volume_path, make_connection
from host_check import get_instance_id

class Compressor:
    def __init__(self, cur):
        self.cur = cur
        self.volume_id = None
        self.member_count = 0
        self.zip_front = None
        self.zip_back = None
        self.volume_threshold = int(get_option('volume_threshold', str(1024 * 1024 * 1024)))
        self.compress_backoff = int(get_option('compress_backoff', str(3600)))

        inst_name = get_option("instance", None)
        self.inst_id = get_instance_id(cur, inst_name)

    def is_full(self):
        if self.zip_back is None:
            return False

        statinfo = os.fstat(self.zip_back.fileno())
        sz = statinfo.st_size
        return sz > self.volume_threshold

    def compress_all(self):
        while True:
            self.compress()
            if self.volume_id is None:
                return

            print("waiting %d seconds..." % (self.compress_backoff,))
            time.sleep(self.compress_backoff)

    def compress(self):
        if self.inst_id is None:
            self.cur.execute("""select field.id
from field
left join content on field.id=content.url_id
where checkd is not null and volume_id is null
order by url_id""")
        else:
            self.cur.execute("""select field.id
from field
join locality on field.id=locality.url_id
left join content on field.id=content.url_id
where checkd is not null and volume_id is null and instance_id=%s
order by content.url_id""", (self.inst_id,))

        rows = self.cur.fetchall()
        for row in rows:
            self.add_member(row[0])
            if self.is_full():
                self.close()

    def add_member(self, url_id):
        self.member_count += 1

        if self.zip_front is None:
            self.volume_id = self.add_volume()
            print("packing volume %d..." % (self.volume_id,))
            archive_path = get_volume_path(self.volume_id)
            self.zip_back = open(archive_path, mode='wb')
            self.zip_front = zipfile.ZipFile(self.zip_back, mode='w', compression=zipfile.ZIP_DEFLATED)

        self.add_member_half(url_id, True)
        self.add_member_half(url_id, False)

        self.cur.execute("""insert into content(url_id, volume_id)
values(%s, %s)""", (url_id, self.volume_id))

    def add_member_half(self, url_id, hdr):
        path = get_loose_path(url_id, hdr)
        if os.path.exists(path):
            self.zip_front.write(path, os.path.basename(path))

    def add_volume(self):
        self.cur.execute("""insert into directory
default values
returning id""")

        row = self.cur.fetchone()
        volume_id = row[0]
        if self.inst_id is not None:
            self.cur.execute("""insert into volume_loc(volume_id, instance_id)
values(%s, %s)""", (volume_id, self.inst_id))

        return volume_id

    def finish_volume(self):
        if self.volume_id is None:
            return

        self.cur.execute("""update directory
set written=localtimestamp
where id=%s""", (self.volume_id,))

        if self.inst_id:
            self.cur.execute("""delete from locality
where url_id in (
        select url_id
        from content
        where volume_id=%s
)""", (self.volume_id,))

        self.cur.execute("""select url_id
from content
where volume_id=%s
order by url_id""", (self.volume_id,))
        rows = self.cur.fetchall()
        for row in rows:
            url_id = row[0]
            self.cond_remove(get_loose_path(url_id))
            self.cond_remove(get_loose_path(url_id, True))

        if self.member_count > 0:
            print("packed %d pages" % (self.member_count,))
            self.member_count = 0

        self.volume_id = None

    def cond_remove(self, doomed):
        if os.path.exists(doomed):
            os.remove(doomed)

    def close(self):
        if self.zip_front is not None:
            self.zip_front.close()
            self.zip_front = None
            self.zip_back.close()
            self.zip_back = None

        self.finish_volume()


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            compressor = Compressor(cur)
            try:
                compressor.compress_all()
            finally:
                compressor.close()

if __name__ == "__main__":
    main()
