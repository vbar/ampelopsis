#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import re
import sys
from common import make_connection
from opt_util import get_quoted_list_option
from timer_base import Occurence, TimerBase

class RedirTimer(TimerBase):
    def __init__(self, cur, deconstructed, segmented):
        TimerBase.__init__(self, cur, deconstructed, segmented)

    def load_item(self, et):
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        target_hamlet_name = et['osobaid']
        target_date = self.extend_date(et)
        target_occ = Occurence(target_hamlet_name, target_date)
        self.add_known(url_id, target_occ)

        self.cur.execute("""select f2.id
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.id=%s""", (url_id,))
        rows = self.cur.fetchall()
        for row in rows:
            source_url_id = row[0]
            source_occ = self.known.get(source_url_id)
            if source_occ is None:
                targets = self.expected.get(source_url_id)
                if targets is None:
                    self.expected[source_url_id] = set((target_occ,))
                else:
                    targets.add(target_occ)
            else:
                self.add_redir(source_occ, target_occ)

    def add_known(self, url_id, occ):
        if url_id in self.known:
            return

        self.known[url_id] = occ

        targets = self.expected.get(url_id)
        if not targets:
            return

        for target_occ in targets:
            self.add_redir(occ, target_occ)

        del self.expected[url_id]

    def add_redir(self, source_occ, target_occ):
        delta = target_occ.date_time - source_occ.date_time
        sec = delta.total_seconds()
        # same datetime for real URL and its redirect dominates,
        # considered incorrect
        if sec > 0:
            self.add_timed_link(source_occ.hamlet_name, target_occ.hamlet_name, sec)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            segmented = (len(sys.argv) == 2) and (sys.argv[1] == '--segmented')
            timer = RedirTimer(cur, parties, segmented)
            try:
                timer.run()
                timer.dump_final_state()
                timer.dump()
            finally:
                timer.close()


if __name__ == "__main__":
    main()
