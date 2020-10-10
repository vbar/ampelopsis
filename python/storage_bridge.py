import re
from common import get_option
from cursor_wrapper import CursorWrapper
from host_check import get_instance_id
from volume_holder import VolumeHolder

content_type_rx = re.compile("^content-type:\\s*(.+)", re.I)

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

    def get_content_type(self, url_id, volume_id):
        reader = self.open_headers(url_id, volume_id)
        if reader:
            try:
                for ln in reader:
                    m = content_type_rx.match()
                    if m:
                        return m.group(1)
            finally:
                reader.close()

        return "text/plain"
