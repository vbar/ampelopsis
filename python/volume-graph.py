#!/usr/bin/python3

# requires database filled by running bottle.py

import json
import sys
from common import make_connection
from palette_factory import create_palette
from palette_lookup import get_membership

class PartyDesc:
    def __init__(self, color, name):
        self.color = color
        self.name = name

    def cond_rename(self, new_name):
        if (self.name is None) or ((new_name is not None) and (len(self.name) > len(new_name))):
            self.name = new_name


class VolumeAccumulator:
    def __init__(self, cur):
        self.cur = cur
        self.palette = create_palette(cur, "ast_party.id")
        self.summary = {} # int year -> int party ID -> int count
        self.legend = {} # int party ID -> PartyDesc
        self.cur.execute("""select ast_party.id, color, party_name
from ast_party
left join ast_party_name on ast_party.id=party_id
order by ast_party.id, party_name""")
        rows = self.cur.fetchall()
        for party_id, color, party_name in rows:
            party_desc = self.legend.get(party_id)
            if party_desc is None:
                self.legend[party_id] = PartyDesc(color, party_name)
            else:
                party_desc.cond_rename(party_name)

    def run(self):
        self.cur.execute("""select speaker_id, speech_day, word_count
from ast_speech
order by speech_day, speech_order""")
        rows = self.cur.fetchall()
        for speaker_id, speech_day, word_count in rows:
            party_id = get_membership(self.palette, speaker_id, speech_day)
            if party_id:
                party2count = self.summary.setdefault(speech_day.year, {})
                cnt = party2count.get(party_id, 0)
                party2count[party_id] = cnt + word_count

    def dump(self):
        summary = []
        legend = {}
        party2total = {}
        for year, party2count in sorted(self.summary.items()):
            item = {
                'year': year
            }

            for party_id, cnt in sorted(party2count.items()):
                old_cnt = party2total.get(party_id, 0)
                party2total[party_id] = old_cnt + cnt
                sid = str(party_id)
                item[sid] = cnt
                if sid not in legend:
                    party_desc = self.legend.get(party_id)
                    if party_desc:
                        name = party_desc.name if party_desc.name else ''
                        color = '#' + party_desc.color if party_desc.color else ''
                        legend[sid] = [ name, color ]

            summary.append(item)

        totals = sorted(party2total.items(), key = lambda p: (-1 * p[1], p[0]))
        data = {
            'summary': summary,
            'legend': legend,
            'order': [ str(p[0]) for p in totals ]
        }

        print(json.dumps(data, indent=2))


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = VolumeAccumulator(cur)
            processor.run()
            processor.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
