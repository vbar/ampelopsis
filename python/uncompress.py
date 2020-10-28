#!/usr/bin/python3

import os
import shutil
import sys
import zipfile
from common import get_loose_path, get_option, get_volume_path, make_connection
from host_check import get_instance_id


class VolumeDecompressor:
    def __init__(self, cur, inst_id, volume_id, volume_path):
        self.cur = cur # no CursorWrapper methods needed here
        self.inst_id = inst_id
        self.volume_id = volume_id
        self.zp = zipfile.ZipFile(volume_path)

    def decompress(self):
        self.cur.execute("""select url_id
from content
where volume_id=%s
order by url_id""", (self.volume_id,))
        rows = self.cur.fetchall()
        for row in rows:
            self.decompress_url(row[0])

        # assuming the database has not changed since query above - no
        # other script may modify it while this one runs
        self.cur.execute("""delete from content
where volume_id=%s""", (self.volume_id,))

        self.cur.execute("""delete from volume_loc
where volume_id=%s""", (self.volume_id,))

        self.cur.execute("""delete from directory
where id=%s""", (self.volume_id,))

    def close(self):
        self.zp.close()

    def decompress_url(self, url_id):
        for hdr in (False, True):
            reader = self.get_reader(url_id, hdr)
            if reader:
                try:
                    self.write_file(url_id, hdr, reader)
                finally:
                    reader.close()

        if self.inst_id:
            self.cur.execute("""insert into locality(url_id, instance_id)
values(%s, %s)
on conflict(url_id) do update
set instance_id=%s""", (url_id, self.inst_id, self.inst_id))

    def get_reader(self, url_id, hdr):
        nm = str(url_id)
        if hdr:
            nm += 'h'

        try:
            info = self.zp.getinfo(nm)
            return self.zp.open(info)
        except KeyError:
            return None

    def write_file(self, url_id, hdr, reader):
        loose_path = get_loose_path(url_id, hdr)
        writer = open(loose_path, "wb")
        try:
            shutil.copyfileobj(reader, writer)
        finally:
            writer.close()

class Decompressor:
    def __init__(self, cur):
        self.cur = cur # no CursorWrapper methods needed here

        inst_name = get_option("instance", None)
        self.inst_id = get_instance_id(cur, inst_name)

    def decompress(self):
        if self.inst_id is None:
            self.cur.execute("""select volume_id
from directory
where written is not null
order by id""")
        else:
            self.cur.execute("""select id
from directory
join volume_loc on id=volume_id
where written is not null and instance_id=%s
order by id""", (self.inst_id,))

        rows = self.cur.fetchall()
        if not len(rows):
            print("no volumes to decompress", file=sys.stderr)
        else:
            for row in rows:
                volume_id = row[0]
                print("decompressing volume %d..." % (volume_id,), file=sys.stderr)
                volume_path = get_volume_path(volume_id)
                vd = VolumeDecompressor(self.cur, self.inst_id, volume_id, volume_path)
                try:
                    vd.decompress()
                finally:
                    vd.close()

                os.remove(volume_path)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            decompressor = Decompressor(cur)
            decompressor.decompress()

if __name__ == "__main__":
    main()
