#!/usr/bin/python3

from lxml import etree
import re
import select
import sys
from urllib.parse import quote_plus, urljoin, urlparse, urlunparse
from common import get_option, get_loose_path, make_connection
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

# www.realhit.cz uses accents in URLs...
def normalize_url_component(path):
    return quote_plus(path, safe="/+%&=[]")

class PageParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.base = url
        self.found_base = False
                
    def parse_links(self, fp):
        # limit memory usage
        context = etree.iterparse(fp, events=('end',), tag=('a', 'base'), html=True, recover=True)
        for action, elem in context:
            if not self.found_base and (elem.tag == 'base'):
                parent = elem.getparent()[0]
                if parent and (parent.tag == 'head'):
                    grandparent = parent.getparent()[0]
                    if grandparent and (grandparent.tag == 'html'):
                        self.found_base = True
                        href = elem.get('href')
                        if href:
                            self.base = urljoin(self.base, href)
            elif elem.tag == 'a':
                href = elem.get('href')
                if href:
                    link = urljoin(self.base, href)
                    self.owner.add_link(link)
                
            # cleanup
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

class MemCache:
    def __init__(self, high_mark, low_mark):
        assert high_mark > low_mark
        assert low_mark > 0
        self.high_mark = high_mark
        self.low_mark = low_mark
        self.cache = {} # url -> count
        
    def check(self, url):
        cnt = self.cache.get(url, 0)
        self.cache[url] = cnt + 1
        if not cnt:
            if len(self.cache) > self.high_mark:
                self.prune()

            return False
        else:
            return True

    def prune(self):
        cache = {}
        lst = sorted([ (v, k) for k, v in self.cache.items() ], reverse=True)
        for v, k in lst[:self.low_mark]:
            cache[k] = v

        self.cache = cache
        
class PolyParser(VolumeHolder, CursorWrapper):
    def __init__(self, single_action, conn, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

        self.mem_cache = MemCache(int(get_option('parse_cache_high_mark', "2000")), int(get_option('parse_cache_low_mark', "1000")))
        self.single_action = single_action
        self.conn = conn
        self.notification_threshold = int(get_option('parse_notification_threshold', "1000"))
        
        page_limit = get_option("page_limit", None)
        self.page_limit = int(page_limit) if page_limit else None
        self.page_count = 0

        # ignore case flag would be better dynamic, but Python 3.5.2
        # doesn't support that...
        url_blacklist_rx = get_option("url_blacklist_rx", "[.](?:jpe?g|pdf|png)$")
        self.url_blacklist_rx = re.compile(url_blacklist_rx, re.I) if url_blacklist_rx else None

        url_whitelist_rx = get_option("url_whitelist_rx", None)
        self.url_whitelist_rx = re.compile(url_whitelist_rx, re.I) if url_whitelist_rx else None
        
        self.host_white = {}
        cur.execute("""select id, hostname
from tops
order by id""")
        rows = cur.fetchall()
        for row in rows:
            self.host_white[row[1]] = row[0]

    def parse_all(self):
        row = self.pop_work_item()
        while row:
            url_id = row[0]
            
            url = self.get_url(url_id)
            if not url:
                print("URL %d not found" % (url_id,), file=sys.stderr)
            else:
                volume_id = self.get_volume_id(url_id)
                self.parse(url_id, url, volume_id)
                
            row = self.pop_work_item()

    def cond_notify(self):
        if not self.single_action:
            self.cur.execute("""select count(*)
from download_queue""")
            row = self.cur.fetchone()
            if row[0] > 0:
                self.cur.execute("""notify download_ready""")
            
    def wait(self):
        self.cur.execute("""LISTEN parse_ready""")
        print("waiting for notification...")
        select.select([self.conn], [], [])
        self.conn.poll()
        print("got %d notification(s)" % (len(self.conn.notifies),))
        while self.conn.notifies:
            self.conn.notifies.pop()
            
    def parse(self, url_id, url, volume_id):
        print("parsing " + url + "...", file=sys.stderr)
        reader = self.open_page(url_id, volume_id)
        if reader:
            try:
                parser = PageParser(self, url)
                parser.parse_links(reader)
            finally:
                reader.close()
        
        self.cur.execute("""update field
set parsed=localtimestamp
where id=%s""", (url_id,))

    def is_done(self):
        return self.page_limit and (self.page_count >= self.page_limit)
    
    def pop_work_item(self):
        if self.is_done():
            return None
        
        # https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/
        self.cur.execute("""delete from parse_queue
where url_id = (
        select url_id
        from parse_queue
        order by url_id
        for update skip locked
        limit 1
)
returning url_id""")
        row = self.cur.fetchone()
        if row:
            self.page_count += 1
            if not (self.page_count % self.notification_threshold):
                self.cond_notify()
                
        return row
    
    def add_link(self, url):
        pr = urlparse(url.strip())
        host_id = self.host_white.get(pr.netloc)
        if host_id:
            clean_pr = (pr.scheme, pr.netloc, normalize_url_component(pr.path), pr.params, normalize_url_component(pr.query), '')
            clean_url = urlunparse(clean_pr)
            
            skip_msg = None
            if self.url_whitelist_rx and not self.url_whitelist_rx.search(clean_url):
                skip_msg = "not whitelisted"
            elif self.url_blacklist_rx and self.url_blacklist_rx.search(clean_url):
                skip_msg = "blacklisted"
            elif len(clean_url) > 512: # should get this from table definition...
                skip_msg = "too long"
                
            if skip_msg:
                print("skipping %s b/c %s" % (clean_url, skip_msg), file=sys.stderr)
            elif not self.mem_cache.check(clean_url):
                self.cur.execute("""insert into field(url) values(%s)
on conflict do nothing
returning id""", (clean_url,))
                row = self.cur.fetchone()
                if row is not None:
                    self.cur.execute("""insert into download_queue(url_id, priority, host_id) values(%s, get_priority(%s), %s)
on conflict do nothing""", (row[0], clean_url, host_id))


def main():
    single_action = (len(sys.argv) == 2) and (sys.argv[1] == '--single-action')
    
    with make_connection() as conn:
        with conn.cursor() as cur:
            parser = PolyParser(single_action, conn, cur)
            try:
                while not parser.is_done():
                    parser.parse_all()
                    if single_action:
                        break
                    else:
                        parser.cond_notify()
                        parser.wait()
            finally:
                parser.close()
            
if __name__ == "__main__":
    main()
        
