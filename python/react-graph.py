#!/usr/bin/python3

# requires download with funnel_links set to 1 and database filled by
# running condensate.py

import collections
import json
import sys
from common import make_connection
from opt_util import get_quoted_list_option
from party_mixin import PartyMixin
from reply_mixin import ReplyMixin
from show_case import ShowCase

Occurence = collections.namedtuple('Occurence', 'hamlet_name date_time')

class ReactionTimer(ShowCase, PartyMixin, ReplyMixin):
    def __init__(self, cur, deconstructed):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        ReplyMixin.__init__(self)

        if deconstructed == '*':
            self.deconstructed = None
        else:
            self.deconstructed = set() # of int party id
            self.init_deconstructed(deconstructed)

        self.known = {} # int url id -> Occurence
        self.expected = {} # int url id -> set of Occurence

        self.variant2react = {}

    def init_deconstructed(self, deco_list):
        if not deco_list:
            return

        deco_set = set(deco_list)
        self.cur.execute("""select party_id
from vn_party_name
where party_name in %s
order by party_id""", (tuple(deco_set),))
        rows = self.cur.fetchall()
        for row in rows:
            self.deconstructed.add(row[0])

    def dump(self):
        desc = []
        reactions = []
        for variant, realn in sorted(self.variant2react.items(), key=lambda p: (-1 * len(p[1]), isinstance(p[0], int), p[0])):
            name = self.get_presentation_name(variant)
            color = self.introduce_color(variant)
            desc.append({'name': name, 'color': color})
            reactions.append(realn)

        custom = {
            'dateExtent': self.make_date_extent(),
            "lineDesc": desc,
            "reactions": reactions
        }

        print(json.dumps(custom, indent=2))

    def dump_final_state(self):
        for url_id, targets in sorted(self.expected.items()):
            url = self.get_url(url_id)
            print(url, file=sys.stderr)
            for target_occ in sorted(targets):
                print("\t" + target_occ.hamlet_name, file=sys.stderr)

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

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]

    def get_variant(self, hamlet_name):
        if self.deconstructed is None:
            return hamlet_name

        party_id = self.hamlet2party.get(hamlet_name)
        if party_id is None:
            return None

        return hamlet_name if party_id in self.deconstructed else party_id

    def get_presentation_name(self, variant):
        if type(variant) is str:
            return self.person_map[variant]
        else:
            return self.party_map[variant]

    def introduce_color(self, variant):
        if type(variant) is str:
            party_id = self.hamlet2party.get(variant, 0)
        else:
            party_id = variant

        return self.convert_color(party_id)


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
