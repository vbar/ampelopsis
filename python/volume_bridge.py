import os
from cursor_wrapper import CursorWrapper
from host_check import get_instance_id
from common import get_option, get_volume_path

class VolumeBridge(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        inst_name = get_option("instance", None)
        self.inst_id = get_instance_id(cur, inst_name)

    def has_local_volume(self, volume_id):
        if self.inst_id is None:
            self.cur.execute("""select count(*)
from directory
left join volume_loc on id=volume_id
where written is not null and instance_id is null""")
        else:
            self.cur.execute("""select count(*)
from directory
join volume_loc on id=volume_id
where written is not null and instance_id=%s""", (self.inst_id,))

        row = self.cur.fetchone()
        return row[0]

    def has_remote_instance(self, volume_id):
        if self.inst_id is None:
            # for DELETE, we require a not-obviously-incorrect configuration
            return False

        self.cur.execute("""select instance_id
from directory
join volume_loc on id=volume_id
where id=%s and written is not null""", (volume_id,))

        rows = self.cur.fetchall()
        found = False
        for row in rows:
            if row[0] == self.inst_id:
                return False

            found = True

        return found

    def get_volume_size(self, volume_id):
        sz = None
        volume_path = get_volume_path(volume_id)
        if os.path.exists(volume_path):
            statinfo = os.stat(volume_path)
            sz = statinfo.st_size

        return sz

    def open_volume(self, volume_id):
        volume_path = get_volume_path(volume_id)
        return open(volume_path, "rb")

    def delete_volume(self, volume_id):
        volume_path = get_volume_path(volume_id)
        if os.path.exists(volume_path):
            os.remove(volume_path)
