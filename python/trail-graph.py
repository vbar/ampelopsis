#!/usr/bin/python3

# requires download with funnel_links set to 3 and database filled by
# running condensate.py

import collections
from itertools import chain
import json
import  math
import sys
from common import make_connection
from json_frame import JsonFrame
from party_mixin import PartyMixin
from trail_mixin import TrailMixin

TrailSummary = collections.namedtuple('TrailSummary', 'max_cursor min_cursor cursor_deltas follower_count checked_follower_count')

class Processor(JsonFrame, PartyMixin, TrailMixin):
    def __init__(self, cur, selected):
        JsonFrame.__init__(self, cur)
        PartyMixin.__init__(self)
        TrailMixin.__init__(self)
        if selected:
            self.restrict_persons()

        self.selected = selected
        self.hamlet2trail = {}

    def run(self):
        for hamlet_name in self.person_map:
            town_name = self.hamlet2town.get(hamlet_name)
            if town_name:
                profile_count = self.get_follower_count(town_name)
                if profile_count is not None:
                    self.load_profile(hamlet_name, town_name, profile_count)

    def dump(self):
        desc = []
        trails = []
        for hamlet_name, src_summa in sorted(self.hamlet2trail.items(), key=lambda p: (-1 * p[1].follower_count, p[0])):
            if src_summa.max_cursor: # not interested in accounts w/o followers
                present_name = self.person_map[hamlet_name]
                party_id = self.hamlet2party.get(hamlet_name, 0)
                desc.append({
                    'name': present_name,
                    'color': self.convert_color(party_id),
                    'followers': src_summa.follower_count,
                    'checkedFollowers': src_summa.checked_follower_count
                })

                if self.selected:
                    dst_summa = src_summa.cursor_deltas
                else:
                    dst_summa = [ src_summa.max_cursor, src_summa.min_cursor ]

                trails.append(dst_summa)

        custom = {
            "lineDesc": desc,
            "lineData": trails
        }

        print(json.dumps(custom, indent=2))

    def load_profile(self, hamlet_name, town_name, profile_count):
        print("walking %s..." % town_name, file=sys.stderr)
        follower_set, trail = self.make_followers_set(town_name, True)
        trail_count = len(follower_set)
        l = len(trail)
        if l == 0:
            ts = TrailSummary(None, None, None, profile_count, trail_count)
        else:
            prv = trail[0]
            deltas = []
            for nxt in trail[1:]:
                if prv <= nxt:
                    raise Exception("trail not descending")

                deltas.append(prv - nxt)
                prv = nxt

            ts = TrailSummary(trail[0], trail[-1], deltas, profile_count, trail_count)

        self.hamlet2trail[hamlet_name] = ts


def main():
    selected = (len(sys.argv) == 2) and (sys.argv[1] == '--selected')
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur, selected)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
