#!/usr/bin/python3

# requires *and invalidates* database filled by running condensate.py
# - run condensate.py again with configured condensate_reset_party
# option to renew it

import random
from common import make_connection
from cursor_wrapper import CursorWrapper

class Shuffle(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        random.seed()
        self.rec2party = {} # vn_record.id => party id
        self.init_map()

    def init_map(self):
        self.cur.execute("""select id, party_id
from vn_record
where party_id is not null
order by id""")
        rows = self.cur.fetchall()
        for row in rows:
            record_id, party_id = row
            self.rec2party[record_id] = party_id

    def shuffle(self):
        seq = [ party_id for record_id, party_id in sorted(self.rec2party.items(), key=lambda p: p[0]) ]
        random.shuffle(seq)

        i = 0
        for record_id in sorted(self.rec2party):
            self.cur.execute("""update vn_record
set party_id=%s
where id=%s""", (seq[i], record_id))
            i += 1


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            shuffle = Shuffle(cur)
            shuffle.shuffle()


if __name__ == "__main__":
    main()
