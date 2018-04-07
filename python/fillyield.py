#!/usr/bin/python3

from common import get_mandatory_option, make_connection
from path_builder import PathBuilder

class Adder(PathBuilder):
    def __init__(self, cur):
        PathBuilder.__init__(self, cur)
        
        print("resetting yields...")
        self.cur.execute("""update nodes
set yield=0""")

    def add(self, url, url_id, depth):
        print("adding %s..." % (url,))
        children = [ url_id ]
        while depth > 0:
            self.add_level(depth, children)
            parents = self.get_parents(children, depth)

            depth -= 1
            children = parents

    def add_level(self, depth, urls):
        w = 1.0 / len(urls)
        self.cur.execute("""update nodes
set yield=yield+%s
where url_id in %s""", (w, tuple(urls)))


def main():
    paydirt = get_mandatory_option('paydirt_rx')
    with make_connection() as conn:
        with conn.cursor() as cur:
            adder = Adder(cur)
            cur.execute("""select url, url_id, depth
from nodes
join field
on url_id=id
where url ~ %s
and depth is not null
order by url""", (paydirt,))
            rows = cur.fetchall()
            for row in rows:
                adder.add(*row)
                
if __name__ == "__main__":
    main()
