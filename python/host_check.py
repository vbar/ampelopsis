from common import get_option
from cursor_wrapper import CursorWrapper

class DefaultCanonicalizer:
    def canonicalize_host(self, raw_host):
        return raw_host

class DomainCanonicalizer:
    def canonicalize_host(self, raw_host):
        segments = raw_host.split(".")
        l = len(segments)
        dl = 2 # probably should be configurable...
        return raw_host if len(segments) <= dl else ".".join(segments[l-dl:l])

def make_canonicalizer():
    return DomainCanonicalizer() if get_option("match_domain", False) else DefaultCanonicalizer()

def get_instance_id(cur, inst_name):
    if not inst_name:
        return None

    cur.execute("""select id
from instances
where instance_name=%s""", (inst_name,))
    row = cur.fetchone()
    if not row:
        raise Exception("Instance %s not in instances" % inst_name)

    return row[0]

def get_parse_notification_name(inst_id):
    return "parse_ready" if inst_id is None else "parse_ready_%d" % inst_id

class HostCheck(CursorWrapper):
    def __init__(self, cur, inst_name=None):
        CursorWrapper.__init__(self, cur)

        self.inst_id = get_instance_id(cur, inst_name)
        self.canonicalizer = make_canonicalizer()

        self.host_white = {}
        if not self.inst_id:
            cur.execute("""select id, hostname
from tops
order by id""")
        else:
            cur.execute("""select id, hostname
from tops
join host_inst on id=host_id
where instance_id=%s
order by id""", (self.inst_id,))
        rows = cur.fetchall()
        for row in rows:
            host = self.canonicalizer.canonicalize_host(row[1])
            self.host_white[host] = row[0]

    def get_host_id(self, host):
        canon_host = self.canonicalizer.canonicalize_host(host)
        return self.host_white.get(canon_host)
