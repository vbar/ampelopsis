#!/usr/bin/python3

from lxml import etree
import select
import sys
from urllib.parse import quote_plus, urljoin, urlparse, urlunparse
import zipfile
from common import get_option, get_volume_path, make_connection

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
                
class PolyParser:
    def __init__(self, single_action, conn, cur):
        self.single_action = single_action
        self.conn = conn
        self.cur = cur
        self.volume_id = None
        self.zp = None
        
        page_limit = get_option("page_limit", None)
        self.page_limit = int(page_limit) if page_limit else None
        self.page_count = 0
        
        self.host_white = set()
        cur.execute("""select hostname
from tops
order by hostname""")
        rows = cur.fetchall()
        for row in rows:
            self.host_white.add(row[0])

    def parse_all(self):
        row = self.pop_work_item()
        while row:
            url_id = row[0]
            
            subrow = self.get_content_spec(url_id)
            if not subrow:
                print("URL %d not found" % (url_id,), file=sys.stderr)
            else:
                self.parse(url_id, *subrow)
                
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
            
    def parse(self, url_id, url, volume_id, member_id):
        print("parsing " + url + "...", file=sys.stderr)
        if volume_id != self.volume_id:
            self.change_volume(volume_id)

        try:
            info = self.zp.getinfo(str(member_id))
        except KeyError:
            info = None

        if info is not None:
            parser = PageParser(self, url)
            with self.zp.open(info) as reader:
                parser.parse_links(reader)
        
        self.cur.execute("""update content
set parsed=localtimestamp
where url_id=%s""", (url_id,))

    def get_content_spec(self, url_id):
        self.cur.execute("""select url, volume_id, member_id
from field
join content on id=url_id
where id=%s""", (url_id,))
        return self.cur.fetchone()

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
            if not (self.page_count % 1000):
                self.cond_notify()
                
        return row
    
    def add_link(self, url):
        pr = urlparse(url.strip())
        if pr.netloc in self.host_white:
            clean_pr = (pr.scheme, pr.netloc, normalize_url_component(pr.path), pr.params, normalize_url_component(pr.query), '')
            clean_url = urlunparse(clean_pr)
            self.cur.execute("""insert into field(url) values(%s)
on conflict do nothing
returning id""", (clean_url,))
            row = self.cur.fetchone()
            if row is not None:
                self.cur.execute("""insert into download_queue(url_id, priority) values(%s, get_priority(%s))
on conflict do nothing""", (row[0], clean_url))

    def change_volume(self, volume_id):
        if self.zp is not None:
            self.zp.close()

        archive_path = get_volume_path(volume_id)
        self.zp = zipfile.ZipFile(archive_path)
        self.volume_id = volume_id
        
    def close(self):
        if self.zp is not None:
            self.zp.close()
            self.zp = None
            
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
        
