#!/usr/bin/python3

import re
import sys
from urllib.parse import urlparse, urlunparse
from common import get_netloc, get_option, make_connection, normalize_url_component
from host_check import HostCheck
from mem_cache import MemCache
from page_parser import PageParser
from volume_holder import VolumeHolder

class Tracer(VolumeHolder, HostCheck):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        HostCheck.__init__(self, cur)
        
        self.mem_cache = MemCache(int(get_option('parse_cache_high_mark', "2000")), int(get_option('parse_cache_low_mark', "1000")))
        
        top_protocols = get_option('top_protocols', 'http https')
        self.protocols = set(re.split('\\s+', top_protocols))
        
    def parse(self, url, url_id):
        print("parsing " + url + "...", file=sys.stderr)
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if reader:
            try:
                parser = PageParser(self, url)
                parser.parse_links(reader)
            finally:
                reader.close()
                
    def add_link(self, url):
        pr = urlparse(url.strip())
        if not pr.scheme in self.protocols:
            return
        
        host_id = self.get_host_id(pr.hostname)
        if not host_id:
            clean_pr = (pr.scheme, get_netloc(pr), normalize_url_component(pr.path), pr.params, normalize_url_component(pr.query), '')
            clean_url = urlunparse(clean_pr)
            if not self.mem_cache.check(clean_url):
                self.cur.execute("""insert into neighbors(url) values(%s)
on conflict do nothing""", (clean_url,))
            
def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            tracer = Tracer(cur)
            try:
                cur.execute("""select url, id
from field
where checkd is not null
order by url""")
                rows = cur.fetchall()
                for row in rows:
                    tracer.parse(*row) 
            finally:
                tracer.close()
            
if __name__ == "__main__":
    main()
        
    
