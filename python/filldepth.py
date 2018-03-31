#!/usr/bin/python3

from common import make_connection

class BreathFirstSearch:
    def __init__(self, cur):
        self.cur = cur

    def check_pre(self):
        self.cur.execute("""select count(*)
from nodes
where depth=0""")
        row = self.cur.fetchone()
        if row[0] == 0:
            raise Exception("root node not set")
        
    def step(self, prev_depth):
        self.cur.execute("""update nodes
set depth=%s
where depth is null and url_id in (
	select to_id
        from edges
        join nodes on from_id=url_id
        where depth=%s
)""", (prev_depth + 1, prev_depth))
        return self.cur.rowcount
    
    def check_post(self):
        # checking extra.has_body would give better results, but since
        # extra is optional, and the rest of this script doesn't need
        # it, it's probably overkill to require it just for a check...
        self.cur.execute("""select count(*)
from nodes
join field on url_id=id
where checkd is not null and depth is null""")
        row = self.cur.fetchone()
        rest = row[0]
        if rest == 0:
            print("all depths set")
        else:
            print("%d nodes have no body/not reachable from depth 0" % (rest,))

            
def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            bfs = BreathFirstSearch(cur)
            # graph.py actually doesn't fill the root (it only fills
            # nodes with parents) - it should be done by seed.py
            bfs.check_pre()
            depth = 0
            count = 1
            while count:
                count = bfs.step(depth)
                depth += 1
                print("found %d nodes at depth %d" % (count, depth))

            bfs.check_post()
            
if __name__ == "__main__":
    main()
