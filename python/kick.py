#!/usr/bin/python3

from common import get_option, make_connection

class Kicker:
    def __init__(self, cur):
        self.cur = cur

    def run(self):
        self.cur.execute("""insert into parse_queue(url_id)
select field.id
from field
left join parse_queue on field.id=parse_queue.url_id
where checkd is not null and parsed is null and parse_queue.url_id is null""")

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            kicker = Kicker(cur)
            kicker.run()

if __name__ == "__main__":
    main()
