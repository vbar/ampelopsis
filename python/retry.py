#!/usr/bin/python3

from common import make_connection
from kick import Kicker
from seed import Seeder

def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            inner_select = """select url_id
from download_error"""

            cur.execute("""update field
set checkd=null
where id in (%s)""" % inner_select)

            cur.execute("""delete from locality
where url_id in (%s)""" % inner_select)
            cur.execute("""delete from content
where url_id in (%s)""" % inner_select)

            cur.execute("delete from download_error")

            kicker = Kicker(cur)
            kicker.run()

            seeder = Seeder(cur)
            seeder.seed_queue()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
