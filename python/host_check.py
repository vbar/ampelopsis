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
    
class HostCheck(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)

        self.canonicalizer = make_canonicalizer()

        self.host_white = {}
        cur.execute("""select id, hostname
from tops
order by id""")
        rows = cur.fetchall()
        for row in rows:
            host = self.canonicalizer.canonicalize_host(row[1])
            self.host_white[host] = row[0]

    def get_host_id(self, host):
        canon_host = self.canonicalizer.canonicalize_host(host)
        return self.host_white.get(canon_host)
        
        
