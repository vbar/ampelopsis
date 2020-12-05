#!/usr/bin/python3

# requires database filled by running condensate.py

import emoji
import json
import random
import re
import regex
from common import get_option, make_connection
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
                row.extend(("U+" + re.sub("^0x", "", hex(ord(e))) for e in emo))
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
        url = et['url']
        emomap = self.vocabulary.setdefault(variant, {})

        # https://stackoverflow.com/questions/43146528/how-to-extract-all-the-emojis-from-text/43146653
        data = regex.findall(r'\X', et['text'])
        for word in data:
            if any(char in emoji.UNICODE_EMOJI for char in word):
                payload = emomap.get(word)
                if payload is None:
                    emomap[word] = Payload(url)
                else:
                    payload.append(url)

    def make_date_extent(self):
        return [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
