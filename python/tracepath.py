#!/usr/bin/python3

import sys
from common import make_connection
from cursor_wrapper import CursorWrapper

class Builder(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        self.path = {} # depth -> [ url_id ]
        
    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            raise Exception("unknown URL " + url)

        return row[0]

    def get_url_depth(self, url_id):
        self.cur.execute("""select depth
from nodes
where url_id=%s""", (url_id,))
        row = self.cur.fetchone()
        return row[0]
    
    def get_parents(self, urls, child_depth):
        self.cur.execute("""select distinct from_id
from edges
join nodes on from_id=url_id
where to_id in %s and depth=%s
order by from_id""", (tuple(urls), child_depth - 1))
        rows = self.cur.fetchall()
        return [ row[0] for row in rows ]

    def add(self, depth, urls):
        self.path[depth] = urls

    def dump(self):
        for depth, ids in self.path.items():
            urls = [ self.get_url(url_id) for url_id in ids ]
            for url in sorted(urls):
                print("%d\t%s" % (depth, url))
                
                
def main():
    if len(sys.argv) != 2:
        raise Exception("usage: " + sys.argv[0] + " URL")

    with make_connection() as conn:
        with conn.cursor() as cur:
            builder = Builder(cur)

            target_id = builder.get_url_id(sys.argv[1])
            depth = builder.get_url_depth(target_id)
            children = [ target_id ]
            while depth > 0:
                print("%d (%d)..." % (depth, len(children)), file=sys.stderr)
                builder.add(depth, children)
                parents = builder.get_parents(children, depth)

                depth -= 1
                children = parents

            builder.add(0, children)
            builder.dump()
            
if __name__ == "__main__":
    main()
