import collections
import json
import sys
from person_party_mixin import PersonPartyMixin
from show_case import ShowCase

Occurence = collections.namedtuple('Occurence', 'hamlet_name date_time')

class TimerBase(ShowCase, PersonPartyMixin):
    def __init__(self, cur, deconstructed):
        ShowCase.__init__(self, cur)
        PersonPartyMixin.__init__(self, deconstructed)
        self.known = {} # int url id -> Occurence
        self.expected = {} # int url id -> set of Occurence
        self.variant2react = {}

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

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]
