#!/usr/bin/python3

from common import get_mandatory_option, make_connection

class Squisher:
    def __init__(self, cur):
        self.cur = cur

    def add(self, url_id):
        self.cur.execute("""select to_id
from edges
where from_id=%s
order by to_id""", (url_id,))
        rows = self.cur.fetchall()
        if not rows:
            return
        
        children = [ row[0] for row in rows ]
        self.cur.execute("""insert into edge_sets(from_set, to_set)
values(%s, %s)
on conflict(to_set) do update
set from_set=edge_sets.from_set || %s""", ([ url_id ], children, url_id))

        
def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            squisher = Squisher(cur)
            cur.execute("""select url_id
from nodes
order by url_id""")
            rows = cur.fetchall()
            idx = 0
            for row in rows:
                squisher.add(row[0])
                idx += 1
                if not (idx % 10000):
                    print("id %d..." % (idx,))
                
if __name__ == "__main__":
    main()
        
