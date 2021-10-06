#!/usr/bin/python3

import csv
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper

class Dumper(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)

    def dump(self, writer):
        headings = ["hlidacstatu.cz", "refers", "matches"]
        writer.writerow(headings)

        self.cur.execute("""select hamlet_name, vn_identity_hamlet.town_name, vn_identity_town.town_name
from vn_record
left join vn_identity_hamlet on vn_identity_hamlet.record_id=vn_record.id
left join vn_identity_town on vn_identity_town.record_id=vn_record.id
order by hamlet_name""")
        rows = self.cur.fetchall()
        for row in rows:
            writer.writerow(row)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            dumper = Dumper(cur)
            writer = csv.writer(sys.stdout, delimiter=",")
            dumper.dump(writer)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
