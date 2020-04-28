import re
import select
import sys
import time
from common import get_option
from host_check import HostCheck

class DownloadBase(HostCheck):
    def __init__(self, conn, cur, single_action):
        # download (as opposed to parsing) host check is restricted by instance
        HostCheck.__init__(self, cur, get_option("instance", None))

        self.conn = conn
        self.single_action = single_action
        self.host_id = 0
        self.max_host_id = self.get_max_host()

        # according to HTTP spec, Retry-After can also have absolute
        # time value, but that had not been seen yet
        self.relative_rx = re.compile("^([0-9]{1,3})$")
        self.holds = {} # host_id -> int time in secs
        self.last_expiration = int(time.time() + 0.5)
        self.counter = 0
        self.healthcheck_interval = int(get_option("healthcheck_interval", "100"))
        self.healthcheck_tail = int(get_option("healthcheck_tail", "100"))
        self.healthcheck_threshold = int(get_option("healthcheck_threshold", "80"))
        if (self.healthcheck_tail <= 0) or (self.healthcheck_threshold <= 0):
            # disabled
            self.healthcheck_interval = 0

    def get_max_host(self):
        mx = 0
        for _, host_id in self.host_white.items():
            if host_id > mx:
                mx = host_id

        return mx

    def get_available_hosts(self):
        self.cond_expire()
        avail = set(self.host_white.values())
        return avail.difference(self.holds.keys())

    def has_holds(self):
        return len(self.holds) > 0

    def add_hold(self, hostname, retry_after):
        host_id = self.get_host_id(hostname)
        if host_id:
            m = self.relative_rx.match(retry_after)
            if m:
                relative = int(m.group(1))
                now = self.cond_expire()
                future = now + relative
                old = self.holds.get(host_id)
                if (old is None) or (old < future):
                    self.holds[host_id] = future
                    return True
            else:
                print("do not understand Retry-After: " + retry_after, file=sys.stderr)
        else:
            print("no hold on blacklisted " + hostname, file=sys.stderr)

        return False

    def pop_work_item(self):
        avail = self.get_available_hosts()
        if not len(avail):
            return None

        last_tops = None
        while True:
            tops = [ hid for hid in avail if hid > self.host_id ]
            tops.sort()
            row = None
            if len(tops) and (tops != last_tops):
                last_tops = tops
                # https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/
                self.cur.execute("""delete from download_queue
where url_id = (
        select url_id
        from download_queue
        where host_id = any(%s)
        order by host_id, priority, url_id
        for update skip locked
        limit 1
)
returning url_id""", (tops,))
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

            if row is None:
                if self.host_id > 0:
                    self.host_id = 0
                else:
                    return None

    def add_redirect(self, url_id, new_url):
        known = False
        new_url_id = None
        while new_url_id is None:
            self.cur.execute("""select id
from field
where url=%s""", (new_url,))
            row = self.cur.fetchone()
            if row is not None:
                new_url_id = row[0]
                known = True
            else:
                self.cur.execute("""insert into field(url) values(%s)
on conflict(url) do nothing
returning id""", (new_url,))
                row = self.cur.fetchone()
                # conflict probably won't happen, but theoretically
                # it's possible that a parallel download inserted the
                # URL since the select above, in which case we'll just
                # try again...
                if row is None:
                    print("parallel insert for " + new_url, file=sys.stderr)
                else:
                    new_url_id = row[0]

        self.cur.execute("""insert into redirect(from_id, to_id) values(%s, %s)
on conflict do nothing""", (url_id, new_url_id))

        return (new_url_id, known)

    def finish_page(self, url_id, eff_id, has_body):
        self.finish_url(url_id)
        if url_id != eff_id:
            self.finish_url(eff_id)

        if has_body:
            self.cur.execute("""insert into parse_queue(url_id) values(%s)
on conflict(url_id) do nothing""", (eff_id,))

    def finish_url(self, url_id):
        if self.inst_id:
            # The on conflict clause prevents crash on conflict, but
            # changing instance isn't really implemented (or even
            # implementable - e.g. what should be done to a previous
            # file in another instance, and how?)... Let's hope
            # conflict will only happen after re-seeding, when old and
            # new instance are the same...
            self.cur.execute("""insert into locality(url_id, instance_id)
values(%s, %s)
on conflict(url_id) do nothing
returning url_id""", (url_id, self.inst_id))
            row = self.cur.fetchone()
            if row is None:
                cur.execute("""select instance_id
from locality
where url_id=%s""", (url_id,))
                row = self.cur.fetchone()
                if row[0] != self.inst_id:
                    raise Exception("Cannot change instance of %d (from %d to %d)." % (url_id, row[0], self.inst_id))

        self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (url_id,))

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
        timeout = self.get_interval()
        self.cur.execute("""listen download_ready""")
        if timeout is None:
            print("waiting for notification...", file=sys.stderr)
            select.select([self.conn], [], [])
        else:
            print("waiting for %d second(s)..." % timeout, file=sys.stderr)
            select.select([self.conn], [], [], timeout)

        self.conn.poll()
        print("got %d notification(s)" % (len(self.conn.notifies),), file=sys.stderr)
        while self.conn.notifies:
            self.conn.notifies.pop()

    def get_interval(self):
        # do not update holds from the time get_available_hosts had
        # been called - if there was a hold then, we want to see it
        # and get a finite interval
        mn = None
        for _, exp in self.holds.items():
            if (mn is None) or (mn > exp):
                mn = exp

        if mn is None:
            return None

        now = int(time.time() + 0.5)
        interval = mn - now
        return interval if interval > 0 else 1

    def cond_expire(self):
        now = int(time.time() + 0.5)
        if now > self.last_expiration:
            expired_hosts = []
            for host_id, exp in self.holds.items():
                if exp < now:
                    expired_hosts.append(host_id)

            for host_id in expired_hosts:
                del self.holds[host_id]

            self.last_expiration = now

        return now

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
