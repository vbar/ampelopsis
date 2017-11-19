#!/usr/bin/python3

from common import get_option, make_connection

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""insert into parse_queue(url_id)
select field.id
from field
left join parse_queue on field.id=parse_queue.url_id
where checkd is not null and parsed is null and parse_queue.url_id is null""")

if __name__ == "__main__":
    main()
