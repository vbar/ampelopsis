#!/usr/bin/python3

import os
import sys
import zipfile
from common import get_loose_path, get_volume_path, make_connection
from cursor_wrapper import CursorWrapper

class Purger(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        self.shrunk = set() # volume IDs
        self.doomed = set() # URL IDs

    def purge_fast(self, url_id):
        volume_id = self.get_volume_id(url_id)
        if volume_id is None:
            loose_path = get_loose_path(url_id)
            self.ensure_removed(loose_path)

            loose_path += 'h'
            self.ensure_removed(loose_path)
        else:
            self.shrunk.add(volume_id)
            self.doomed.add(url_id)

        self.purge_from_set(url_id)
        self.purge_to_set(url_id)

        self.cur.execute("""delete from locality
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from edges
where from_id=%s or to_id=%s""", (url_id, url_id))

        self.cur.execute("""delete from nodes
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from extra
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from redirect
where from_id=%s or to_id=%s""", (url_id, url_id))

        self.cur.execute("""delete from parse_queue
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from content
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from download_error
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from download_queue
where url_id=%s""", (url_id,))

        self.cur.execute("""delete from field
where id=%s""", (url_id,))

    def purge_from_set(self, url_id):
        self.cur.execute("""delete from edge_sets
where from_set=%s""", ([url_id],))

        self.cur.execute("""update edge_sets
set from_set=array_remove(from_set, %s)""", (url_id,))

    def purge_to_set(self, url_id):
        self.cur.execute("""delete from edge_sets
where to_set=%s""", ([url_id],))

        self.cur.execute("""select from_set, to_set
from edge_sets
where %s = any(to_set)""", (url_id,))
        rows = self.cur.fetchall()
        for parents, children in rows:
            mod_children = [ uid for uid in children if uid != url_id ]
            mod_parents = self.get_parents(mod_children)
            if mod_parents is None:
                self.cur.execute("""insert into edge_sets(from_set, to_set)
values(%s, %s)""", (parents, mod_children))
            else:
                all_parents = set(parents)
                all_parents.update(mod_parents)
                new_parents = sorted(all_parents)
                self.cur.execute("""update edge_sets
set from_set=%s
where to_set=%s""", (new_parents, mod_children))

            self.cur.execute("""delete from edge_sets
where to_set=%s""", (children,))

    def get_parents(self, children):
        self.cur.execute("""select from_set
from edge_sets
where to_set=%s""", (children,))
        row = self.cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def ensure_removed(path):
        if os.path.exists(path):
            os.remove(path)

    def purge_rest(self):
        for volume_id in self.shrunk:
            self.shrink_volume(volume_id)

    def is_doomed(self, filename):
        stem = filename[:-1] if filename.endswith('h') else filename
        url_id = int(stem)
        return url_id in self.doomed

    def shrink_volume(self, volume_id):
        archive_path = get_volume_path(volume_id)
        backup_path = archive_path + '.bak'
        os.rename(archive_path, backup_path)

        zin = zipfile.ZipFile(backup_path)
        zout = zipfile.ZipFile(archive_path, mode='w', compression=zipfile.ZIP_DEFLATED)
        remains = False
        for item in zin.infolist():
            if not self.is_doomed(item.filename):
                 buf = zin.read(item.filename)
                 zout.writestr(item, buf)
                 remains = True

        zout.close()
        zin.close()

        os.remove(backup_path)

        if not remains:
            self.cur.execute("""delete from volume_loc
where volume_id=%s""", (volume_id,))
            self.cur.execute("""delete from directory
where id=%s""", (volume_id,))
            os.remove(archive_path)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            purger = Purger(cur)
            for url in sys.argv[1:]:
                cur.execute("""select id
from field
where url=%s""", (url,))
                row = cur.fetchone()
                if not row:
                    print(url + " not found", file=sys.stderr)
                else:
                    purger.purge_fast(row[0])

            purger.purge_rest()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
