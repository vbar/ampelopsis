import sys
from cursor_wrapper import CursorWrapper
from show_case import ShowCase

class ActivitySelector(ShowCase):
    def __init__(self, cur, top_limit):
        ShowCase.__init__(self, cur)

        if top_limit <= 0:
            raise Exception("top contributors limit must be positive")

        self.hamlet2count = {} # str -> int
        self.top_limit = top_limit

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            hamlet_name = et.get('osobaid')
            count = self.hamlet2count.get(hamlet_name, 0)
            self.hamlet2count[hamlet_name] = count + 1

    def get_selected_contributors(self):
        # FIXME: use heap
        contributors = [ contr for contr, cnt in sorted(self.hamlet2count.items(), key=lambda kv: -1 * kv[1]) ]
        if len(contributors) < self.top_limit:
            return contributors
        else:
            return contributors[:self.top_limit]


class PartySelector(CursorWrapper):
    def __init__(self, cur, party_names):
        CursorWrapper.__init__(self, cur)

        if not len(party_names):
            raise Exception("no selected parties")

        self.white_party_names = set(party_names)
        self.hamlet2party = {} # str -> str

    def run(self):
        self.cur.execute("""select hamlet_name, long_name
from vn_record
join vn_party on vn_party.id=party_id
order by hamlet_name""")
        rows = self.cur.fetchall()
        for row in rows:
            hamlet_name, party_name = row
            if party_name in self.white_party_names:
                self.hamlet2party[hamlet_name] = party_name

    def get_selected_contributors(self):
        return self.hamlet2party.keys()

    def close(self):
        pass
