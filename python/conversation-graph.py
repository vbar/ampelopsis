#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import collections
import json
import random
import sys
from urllib.parse import urlparse
from common import get_option, make_connection
from party_mixin import by_reverse_value
from reply_mixin import ReplyMixin
from timer_base import TimerBase

StatusOcc = collections.namedtuple('StatusOcc', 'url_id hamlet_name date_time')


class Section:
    def __init__(self, url_id):
        self.count = 1
        self.position = [ url_id ]

    def append(self, url_id):
        self.count += 1
        self.position.append(url_id)

    def shuffle(self):
        random.shuffle(self.position)


class Processor(TimerBase, ReplyMixin):
    def __init__(self, cur):
        TimerBase.__init__(self, cur, '*')
        ReplyMixin.__init__(self)
        random.seed()
        self.size_threshold = int(get_option("bubline_size_threshold", "10"))
        self.sample_max = int(get_option("bubline_sample_max", "3"))
        self.id2skein = {} # url id -> set of StatusOcc (containing a
                           # StatusOcc with the url id); values are
                           # shared
        self.output = set() # of frozenset of StatusOcc

    def process(self):
        rest = set() # of url id
        for url_id, skein in self.id2skein.items():
            found = False
            for occ in skein:
                if occ.url_id == url_id:
                    found = True

            if not found:
                raise Exception("%d not found in its skein" % url_id)

            ball = frozenset(skein)
            if ball not in self.output:
                self.output.add(ball)
                for occ in skein:
                    if occ.url_id != url_id:
                        rest.add(occ.url_id)
            else:
                rest.remove(url_id)

        l = len(rest)
        if l:
            raise Exception("%d skein URLs not indexed" % l)

        print("%d skein(s)" % len(self.output), file=sys.stderr)
        size2count = {}
        for ball in self.output:
            l = len(ball)
            cnt = size2count.get(l, 0)
            size2count[l] = cnt + 1

        for sz, cnt in sorted(size2count.items(), key=lambda p: (-1 * p[1], -1 * p[0])):
            print("%d x %d" % (cnt, sz), file=sys.stderr)

    def dump(self):
        custom = {
            'colors': self.make_meta(),
            'bubline': self.make_bubline(),
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        target_hamlet_name = et['osobaid']
        target_date = self.extend_date(et)
        target_occ = StatusOcc(url_id, target_hamlet_name, target_date)
        self.add_known(target_occ)

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
                self.add_skein(source_occ, target_occ)

    def add_known(self, occ):
        if occ.url_id in self.known:
            return

        self.known[occ.url_id] = occ

        targets = self.expected.get(occ.url_id)
        if not targets:
            return

        for target_occ in targets:
            self.add_skein(occ, target_occ)

        del self.expected[occ.url_id]

    def add_skein(self, source_occ, target_occ):
        assert source_occ.url_id != target_occ.url_id

        source_skein = self.id2skein.get(source_occ.url_id)
        if source_skein:
            target_skein = self.id2skein.get(target_occ.url_id)
            if target_skein:
                union_skein = source_skein | target_skein
                for occ in union_skein:
                    self.id2skein[occ.url_id] = union_skein
            else:
                source_skein.add(target_occ)
                self.id2skein[target_occ.url_id] = source_skein
        else:
            target_skein = self.id2skein.get(target_occ.url_id)
            if target_skein:
                target_skein.add(source_occ)
                self.id2skein[source_occ.url_id] = target_skein
            else:
                new_skein = set((source_occ, target_occ))
                for occ in new_skein:
                    self.id2skein[occ.url_id] = new_skein

    def make_meta(self):
        person2party = {} # subset of self.hamlet2party, with (party-assigned) participants
        for ball in self.output:
            for occ in ball:
                hamlet_name = occ.hamlet_name
                if hamlet_name not in person2party:
                    party_id = self.hamlet2party.get(hamlet_name)
                    if party_id:
                        person2party[hamlet_name] = party_id

        name2color = {}
        for hamlet_name, party_id in person2party.items():
            name = self.party_map[party_id]
            if name not in name2color:
                # party id is-a variant
                color = self.introduce_color(party_id)
                name2color[name] = color

        return name2color

    def make_bubline(self):
        bubline = []
        for ball in sorted(self.output, key=lambda b: -1 * len(b)):
            if len(ball) >= self.size_threshold:
                bubline.append(self.make_bubble(ball))

        return bubline

    def make_bubble(self, ball):
        bubble = {
            'size': len(ball)
        }

        person2count = {} # str hamlet name -> int
        party2section = {} # int -> Section
        mindate = None
        maxdate = None
        for occ in ball:
            hamlet_name = occ.hamlet_name
            cnt = person2count.get(hamlet_name, 0)
            person2count[hamlet_name] = cnt + 1

            party_id = self.hamlet2party.get(hamlet_name)
            if party_id:
                section = party2section.get(party_id)
                if section is None:
                    party2section[party_id] = Section(occ.url_id)
                else:
                    section.append(occ.url_id)

            cdt = occ.date_time
            assert cdt
            if (mindate is None) or (cdt < mindate):
                mindate = cdt

            if (maxdate is None) or (cdt > maxdate):
                maxdate = cdt

        participants = []
        for hamlet_name, cnt in sorted(person2count.items(), key=by_reverse_value):
            participants.append(self.person_map[hamlet_name])

        bubble['participants'] = participants

        name2detail = {}
        for party_id, section in sorted(party2section.items(), key=lambda p: (-1 * p[1].count, p[0])):
            name = self.party_map[party_id]
            name2detail[name] = self.make_detail(section)

        bubble['party2detail'] = name2detail

        bubble['dateExtent'] = [dt.isoformat() for dt in (mindate, maxdate)]

        return bubble

    def make_detail(self, section):
        section.shuffle()
        samples = [ self.get_url(url_id) for url_id in section.position[:self.sample_max] ]
        return {
            'count': section.count,
            'samples': samples
        }


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.process()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
