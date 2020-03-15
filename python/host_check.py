from urllib.parse import urlparse
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

def allow_immediate_download(extra_header, url):
    pr = urlparse(url)
    if pr.hostname != 'www.hlidacstatu.cz':
        return True

    segments = pr.path.split('/')
    private_path_flag = segments[1] == 'api'
    credential_flag = extra_header is not None
    return private_path_flag == credential_flag

class HostCheck(CursorWrapper):
    def __init__(self, cur, inst_name=None):
        CursorWrapper.__init__(self, cur)

        self.inst_id = get_instance_id(cur, inst_name)
        self.canonicalizer = make_canonicalizer()

        self.host_white = {}
        sql_cond = ""
        if self.inst_id:
            sql_cond = "where instance_id=%d" % self.inst_id

        cur.execute("""select id, hostname
from tops
%s
order by id""" % (sql_cond,))
        rows = cur.fetchall()
        for row in rows:
            host = self.canonicalizer.canonicalize_host(row[1])
            self.host_white[host] = row[0]

    def get_host_id(self, host):
        canon_host = self.canonicalizer.canonicalize_host(host)
        return self.host_white.get(canon_host)
