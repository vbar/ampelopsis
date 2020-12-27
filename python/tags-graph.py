#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import random
import re
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase

class TermFreqPayload:
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
        self.sample_max = int(get_option("table_sample_max", "3"))
        self.tag_rx = re.compile('#([-\\w]+)')
        self.vocabulary = {} # variant -> tag str -> TermFreqPayload

    def dump(self):
        bulk = {}
        for variant, tagmap in self.vocabulary.items():
            bulk[variant] = sum((p.count for p in tagmap.values()))

        data = []
        for variant, tagmap in sorted(self.vocabulary.items(), key=lambda p: (-1 * bulk[p[0]], str(p[0]))):
            present_name = self.get_presentation_name(variant)
            terms = []
            samples = []
            for tag, payload in sorted(tagmap.items(), key=lambda p: (-1 * p[1].count, str(p[0]))):
                terms.append(['#' + tag, payload.count])

                sample_list = list(payload.samples)
                if len(sample_list) > self.sample_max:
                    random.shuffle(sample_list)

                samples.append(sample_list[0:self.sample_max])

            assert len(terms)
            item = {
                'name': present_name,
                'color': self.introduce_color(variant),
                'terms': terms,
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
        tagmap = self.vocabulary.get(variant)
        for m in self.tag_rx.finditer(et['text']):
            if tagmap is None:
                tagmap = {}
                self.vocabulary[variant] = tagmap

            term = m.group(1)
            payload = tagmap.get(term)
            if payload is None:
                tagmap[term] = TermFreqPayload(url)
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
