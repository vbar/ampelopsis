import select
import sys
from common import get_option
from cursor_wrapper import CursorWrapper

class DownloadBase(CursorWrapper):
    def __init__(self, conn, cur, single_action):
        CursorWrapper.__init__(self, cur)

        self.conn = conn
        self.single_action = single_action
        self.host_id = 0
        self.max_host_id = self.get_max_host()
        self.counter = 0
        self.healthcheck_interval = int(get_option("healthcheck_interval", "100"))
        self.healthcheck_tail = int(get_option("healthcheck_tail", "100"))
        self.healthcheck_threshold = int(get_option("healthcheck_threshold", "80"))
        if (self.healthcheck_tail <= 0) or (self.healthcheck_threshold <= 0):
            # disabled
            self.healthcheck_interval = 0

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

                if self.healthcheck_interval > 0:
                    self.counter += 1
                    if (self.counter >= self.healthcheck_tail) and not(self.counter % self.healthcheck_interval):
                        self.healthcheck()

                return row
            else:
                if self.host_id > 0:
                    self.host_id = 0
                else:
                    return None

    def cond_notify(self):
        live = False
        if not self.single_action:
            self.cur.execute("""select count(*)
from parse_queue""")
            row = self.cur.fetchone()
            live = row[0] > 0
            if live:
                self.do_notify()

        return live

    def do_notify(self):
        self.cur.execute("""notify parse_ready""")

    def wait(self):
        self.cur.execute("""listen download_ready""")
        print("waiting for notification...")
        select.select([self.conn], [], [])
        self.conn.poll()
        print("got %d notification(s)" % (len(self.conn.notifies),))
        while self.conn.notifies:
            self.conn.notifies.pop()

    def healthcheck(self):
        print("checking success rate...", file=sys.stderr)
        q = """select count(*)
from (
        select id
        from field
        where checkd is not null
        order by checkd desc
        limit %d
) sq
join download_error on id=url_id""" % self.healthcheck_tail
        self.cur.execute(q)
        row = self.cur.fetchone()
        err_count = row[0]
        msg = "on the last %d downloads, failed on %d" % (self.healthcheck_tail, err_count)
        if err_count >= self.healthcheck_threshold:
            raise Exception(msg)
        elif err_count > 0:
            print(msg, file=sys.stderr)
