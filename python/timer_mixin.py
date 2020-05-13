import collections
import sys

Occurence = collections.namedtuple('Occurence', 'hamlet_name date_time')

class TimerMixin:
    def __init__(self):
        self.known = {} # int url id -> Occurence
        self.expected = {} # int url id -> set of Occurence

    def add_known(self, url_id, occ):
        if url_id in self.known:
            return

        self.known[url_id] = occ

        targets = self.expected.get(url_id)
        if not targets:
            return

        for target_occ in targets:
            self.add_resolved(occ, target_occ)

        del self.expected[url_id]

    def dump_final_state(self):
        for url_id, targets in sorted(self.expected.items()):
            url = self.get_url(url_id)
            print(url, file=sys.stderr)
            for target_occ in sorted(targets):
                print("\t" + target_occ.hamlet_name, file=sys.stderr)
