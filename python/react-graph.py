#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

from common import make_connection
from opt_util import get_quoted_list_option
from reply_mixin import ReplyMixin
from timer_base import Occurence, TimerBase

class ReactionTimer(TimerBase, ReplyMixin):
    def __init__(self, cur, deconstructed):
        TimerBase.__init__(self, cur, deconstructed)
        ReplyMixin.__init__(self)

    def load_item(self, et):
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        target_hamlet_name = et['osobaid']
        target_date = self.extend_date(et)
        target_occ = Occurence(target_hamlet_name, target_date)
        self.add_known(url_id, target_occ)

        if self.is_redirected(url):
            return

        root = self.get_html_document(url_id)
        ancestors = self.get_ancestors(root)
        for source_url_id in ancestors:
            source_occ = self.known.get(source_url_id)
            if source_occ is None:
                targets = self.expected.get(source_url_id)
                if targets is None:
                    self.expected[source_url_id] = set((target_occ,))
                else:
                    targets.add(target_occ)
            else:
                self.add_reaction(source_occ, target_occ)

    def add_known(self, url_id, occ):
        if url_id in self.known:
            return

        self.known[url_id] = occ

        targets = self.expected.get(url_id)
        if not targets:
            return

        for target_occ in targets:
            self.add_reaction(occ, target_occ)

        del self.expected[url_id]

    def add_reaction(self, source_occ, target_occ):
        if source_occ.hamlet_name == target_occ.hamlet_name:
            return

        variant = self.get_variant(target_occ.hamlet_name)
        if not variant:
            return

        reactions = self.variant2react.get(variant)
        if not reactions:
            reactions = []
            self.variant2react[variant] = reactions

        delta = target_occ.date_time - source_occ.date_time
        reactions.append(delta.total_seconds())


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            timer = ReactionTimer(cur, parties)
            try:
                timer.run()
                timer.dump_final_state()
                timer.dump()
            finally:
                timer.close()


if __name__ == "__main__":
    main()