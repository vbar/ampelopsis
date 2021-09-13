#!/usr/bin/python3

import csv
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper

class Checker(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)

    def dump(self, writer):
        writer.writerow(['url', 'count'])
        
        self.cur.execute("""select url, tt.c 
from field
join (
    select count(from_id) as c, to_id
    from redirect
    group by to_id ) as tt on id=tt.to_id
order by tt.c desc""")
        rows = self.cur.fetchall()
        for row in rows:
            writer.writerow([row[0], row[1]])

                
def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            checker = Checker(cur)
            writer = csv.writer(sys.stdout, delimiter=",")
            checker.dump(writer)


if __name__ == "__main__":
    main()
