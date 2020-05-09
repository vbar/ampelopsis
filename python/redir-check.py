#!/usr/bin/python3

# requires download with funnel_links set (to at least 1)

from common import make_connection
from cursor_wrapper import CursorWrapper

class Checker(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)

    def check_cycle(self):
        self.cur.execute("""select url
from redirect r1
join redirect r2 on r1.to_id=r2.from_id
join field on r1.to_id=id""")
        rows = self.cur.fetchall()
        for row in rows:
            print(row[0])

        return len(rows)

    def check_freq(self):
        self.cur.execute("""select count(from_id) c, url
from redirect
join field on to_id=id
group by url
having count(from_id)>1
order by c desc""")
        rows = self.cur.fetchall()
        for cnt, url in rows:
            print(url, "\t", cnt)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            checker = Checker(cur)
            if not checker.check_cycle():
                checker.check_freq()


if __name__ == "__main__":
    main()
