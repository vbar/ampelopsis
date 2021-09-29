#!/usr/bin/python3

# requires database filled by running condensate.py

import csv
import json
import random
import sys
from common import get_option, make_connection
from known_names import KnownNames
from lang_wrap import init_lang_recog
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase
from token_util import tokenize

class Payload:
    def __init__(self, owner, url):
        self.owner = owner
        self.count = 1
        self.samples = [ url ]

    def append(self, url):
        self.count += 1

        r = random.random()
        if r < self.owner.add_threshold:
            self.samples.append(url)
        elif r < self.owner.replace_threshold:
            d = random.randrange(len(self.samples))
            del self.samples[d]
            self.samples.append(url)


class Processor(PinholeBase):
    def __init__(self, cur, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        random.seed()
        self.lang_recog = init_lang_recog()
        self.add_threshold = float(get_option("lang_mem_add_threshold", "0.001"))
        self.replace_threshold = self.add_threshold + float(get_option("lang_mem_replace_threshold", "0.5"))
        self.lang2total = {} # str lang -> int
        self.variant2langmap = {} # str hamlet name / int party id -> str lang -> Payload

    def dump_meta(self, output_path):
        meta = {
            'colors': self.make_meta(),
            'totals': self.lang2total,
            'samples': self.make_samples(),
            # old D3 in frontend doesn't parse ISO format...
            'dateExtent': [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]
        }

        with open(output_path, 'w') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

    def dump_content(self, output_path):
        tail_keys = sorted(self.lang2total.keys(), key=lambda k: -1 * self.lang2total[k])
        with open(output_path, 'w') as f:
            writer = csv.writer(f, delimiter=",")
            headings = [ KnownNames.NAME ]
            headings.extend(tail_keys)
            writer.writerow(headings)
            for variant, langmap in self.variant2langmap.items():
                row = [ self.get_presentation_name(variant) ]
                for lng in tail_keys:
                    cnt = 0
                    payload = langmap.get(lng)
                    if payload:
                        cnt = payload.count

                    row.append(cnt)

                writer.writerow(row)

    def load_item(self, et):
        self.extend_date(et)
        url = self.get_circuit_url(et)
        hamlet_name = et['osobaid']
        variant = self.get_variant(hamlet_name)
        if not variant:
            variant = 0

        self.introduce_node(variant, False)

        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if not lng:
            lng = 'other'

        ttl = self.lang2total.get(lng, 0)
        self.lang2total[lng] = ttl + 1

        langmap = self.variant2langmap.get(variant)
        if langmap is None:
            langmap = { lng: Payload(self, url) }
            self.variant2langmap[variant] = langmap
        else:
            payload = langmap.get(lng)
            if payload is None:
                langmap[lng] = Payload(self, url)
            else:
                payload.append(url)

    def get_presentation_name(self, variant):
        if variant == 0:
            return KnownNames.OTHER_NAME

        return PinholeBase.get_presentation_name(self, variant)

    def make_meta(self):
        name2color = {}
        n = len(self.pair2node)
        for i in range(n):
            variant = self.node2variant[i]
            name = self.get_presentation_name(variant)
            color = self.introduce_color(variant)
            name2color[name] = color

        return name2color

    def make_samples(self):
        samples = {}
        for variant, langmap in self.variant2langmap.items():
            name = self.get_presentation_name(variant)
            lang2samples = {}
            for lng, payload in langmap.items():
                lang2samples[lng] = payload.samples

            samples[name] = lang2samples

        return samples


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()

                json_target = get_option("lang_meta_data", "lang.json")
                processor.dump_meta(json_target)
                csv_target = get_option("lang_data", "lang.csv")
                processor.dump_content(csv_target)
            finally:
                processor.close()


if __name__ == "__main__":
    main()
