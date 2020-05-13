import json
import sys
from person_party_mixin import PersonPartyMixin
from show_case import ShowCase
from timer_mixin import Occurence, TimerMixin

class TimerBase(ShowCase, PersonPartyMixin, TimerMixin):
    def __init__(self, cur, deconstructed, segmented=False):
        ShowCase.__init__(self, cur)
        PersonPartyMixin.__init__(self, deconstructed)
        TimerMixin.__init__(self)
        self.segmented = segmented
        self.variant2react = {}

    def add_timed_link(self, source_name, target_name, time_sec):
        if source_name == target_name:
            return

        variant = self.get_variant(target_name)
        if not variant:
            return

        reactions = self.variant2react.get(variant)
        if not reactions:
            reactions = []
            self.variant2react[variant] = reactions

        if self.segmented:
            source_party = self.hamlet2party.get(source_name)
            target_party = self.hamlet2party.get(target_name)
            same = (source_party is not None) and (target_party is not None) and (source_party == target_party)
            reactions.append((time_sec, same))
        else:
            reactions.append(time_sec)

    def dump(self):
        desc = []
        reactions = []
        for variant, realn in sorted(self.variant2react.items(), key=lambda p: (-1 * len(p[1]), isinstance(p[0], int), p[0])):
            name = self.get_presentation_name(variant)
            color = self.introduce_color(variant)
            desc.append({'name': name, 'color': color})

            if self.segmented:
                same = []
                other = []
                for timespan, flag in realn:
                    if flag:
                        same.append(timespan)
                    else:
                        other.append(timespan)

                reactions.append([same, other])
            else:
                reactions.append(realn)

        custom = {
            'dateExtent': self.make_date_extent(),
            "lineDesc": desc,
            "reactions": reactions
        }

        print(json.dumps(custom, indent=2))

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]
