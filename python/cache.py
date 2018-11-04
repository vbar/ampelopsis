#!/usr/bin/python3

from common import make_connection
from purge import Purger
from seed import Seeder

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            purger = Purger(cur)
            cur.execute("""select id from field
where (id > 2) and (url like 'https://cro.justice.cz/verejnost/api/funkcionari?order=DESC%')
order by url""")
            rows = cur.fetchall()
            for row in rows:
                purger.purge_fast(row[0])

            purger.purge_rest()

            cur.execute("""insert into field_cache(id, url, checkd)
select field.id, field.url, field.checkd
from field
left join field_cache on field.url=field_cache.url
where (field.id > 2) and (field.checkd is not null) and (field_cache.id is null)""")

            cur.execute("""delete from field
where (id > 2) and (checkd is not null)""")

            cur.execute("""update field
set checkd=null
where id=1""")

            seeder = Seeder(cur)
            seeder.seed_queue()

if __name__ == "__main__":
    main()
