#!/usr/bin/python3

from common import make_connection
from kick import Kicker
from seed import Seeder

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            inner_select = """select url_id
from download_error
where error_code<>404""" # if a page does not exist, asking for it
                         # again is probably a waste of time...

            cur.execute("""update field
set checkd=null
where id in (%s)""" % inner_select)

            cur.execute("""delete from locality
where url_id in (%s)""" % inner_select)
            cur.execute("""delete from content
where url_id in (%s)""" % inner_select)

            cur.execute("""delete from download_error
where error_code<>404""")

            kicker = Kicker(cur)
            kicker.run()

            seeder = Seeder(cur)
            seeder.seed_queue()

if __name__ == "__main__":
    main()
