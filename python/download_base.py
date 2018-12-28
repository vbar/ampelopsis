from cursor_wrapper import CursorWrapper

class DownloadBase(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        self.host_id = 0
        self.max_host_id = self.get_max_host()

    def get_max_host(self):
        self.cur.execute("""select max(id)
from tops""")
        row = self.cur.fetchone()
        return row[0]

    def pop_work_item(self):
        while True:
            # https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/
            self.cur.execute("""delete from download_queue
where url_id = (
        select url_id
        from download_queue
        where host_id > %s
        order by host_id, priority, url_id
        for update skip locked
        limit 1
)
returning url_id""", (self.host_id,))
            row = self.cur.fetchone()
            if row:
                self.host_id += 1
                if self.host_id >= self.max_host_id:
                    self.host_id = 0

                return row
            else:
                if self.host_id > 0:
                    self.host_id = 0
                else:
                    return None
