import os
import re
from common import get_loose_path, get_option
from cursor_wrapper import CursorWrapper
from host_check import get_instance_id
from volume_holder import VolumeHolder

content_type_rx = re.compile(b"^content-type:\\s*(.+)", re.I)

class StorageBridge(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        inst_name = get_option("instance", None)
        self.inst_id = get_instance_id(cur, inst_name)

    def has_local_data(self, url_id):
        if self.inst_id is None:
            self.cur.execute("""select count(*)
from field
left join download_error on id=download_error.url_id
left join locality on id=locality.url_id
where id=%s and checkd is not null and failed is null and instance_id is null""", (url_id,))
        else:
            self.cur.execute("""select count(*)
from field
left join download_error on id=download_error.url_id
join locality on id=locality.url_id
where id=%s and checkd is not null and failed is null and instance_id=%s""", (url_id, self.inst_id))

        row = self.cur.fetchone()
        return row[0]

    def has_remote_instance(self, url_id):
        if self.inst_id is None:
            # for DELETE, we require a not-obviously-incorrect configuration
            return False

        self.cur.execute("""select instance_id
from field
left join download_error on id=download_error.url_id
join locality on id=locality.url_id
where id=%s and checkd is not null and failed is null and instance_id is not null""", (url_id,))

        rows = self.cur.fetchall()
        found = False
        for row in rows:
            if row[0] == self.inst_id:
                return False

            found = True

        return found

    def get_headers_size(self, url_id, volume_id=None):
        sz = None
        if volume_id is None:
            loose_path = get_loose_path(url_id, True)
            if os.path.exists(loose_path):
                statinfo = os.stat(loose_path)
                sz = statinfo.st_size
        else:
            if volume_id != self.volume_id:
                self.change_volume(volume_id)

            try:
                info = self.zp.getinfo(str(url_id) + 'h')
                sz = info.file_size
            except KeyError:
                pass

        return sz

    def get_content_type(self, url_id, volume_id):
        reader = self.open_headers(url_id, volume_id)
        if reader:
            try:
                for ln in reader:
                    m = content_type_rx.match(ln)
                    if m:
                        b = m.group(1)
                        return b.decode('utf-8')
            finally:
                reader.close()

        return "text/plain"

    def delete_storage(self, url_id):
        for hdr in (True, False):
            loose_path = get_loose_path(url_id, hdr)
            if os.path.exists(loose_path):
                os.remove(loose_path)
