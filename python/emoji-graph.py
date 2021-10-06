#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import random
from common import get_option, make_connection
from emoji_util import get_emojis, get_emoji_hex
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase

class Payload:
    def __init__(self, url):
        self.count = 1
        self.samples = set((url,))

    def append(self, url):
        self.count += 1
        self.samples.add(url)


class Processor(PinholeBase):
    def __init__(self, cur, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        random.seed()
        self.sample_max = int(get_option("emoji_sample_max", "3"))
        self.vocabulary = {} # variant -> emoji str -> Payload

    def dump(self):
        bulk = {}
        for variant, emomap in self.vocabulary.items():
            bulk[variant] = sum((p.count for p in emomap.values()))

        data = []
        for variant, emomap in sorted(self.vocabulary.items(), key=lambda p: (-1 * bulk[p[0]], str(p[0]))):
            present_name = self.get_presentation_name(variant)
            emojis = []
            samples = []
            for emo, payload in sorted(emomap.items(), key=lambda p: (-1 * p[1].count, str(p[0]))):
                row = [ emo ]
                row.extend(get_emoji_hex(emo))
                emojis.append(row)

                sample_list = list(payload.samples)
                if len(sample_list) > self.sample_max:
                    random.shuffle(sample_list)

                samples.append(sample_list[0:self.sample_max])

            if len(emojis):
                item = {
                    'name': present_name,
                    'color': self.introduce_color(variant),
                    'emojis': emojis,
                    'samples': samples
                }

                data.append(item)

        custom = {
            'data': data,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2, ensure_ascii=False))

    def load_item(self, et):
        url = et['url']
        if self.is_redirected(url):
            return

        hamlet_name = et['osobaid']
        variant = self.get_variant(hamlet_name)
        if not variant:
            return

        self.extend_date(et)
        url = self.get_circuit_url(et['url'])
        emomap = self.vocabulary.setdefault(variant, {})

        emoji_list = get_emojis(et['text'])
        for word in emoji_list:
            payload = emomap.get(word)
            if payload is None:
                emomap[word] = Payload(url)
            else:
                payload.append(url)

    def make_date_extent(self):
        return [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
