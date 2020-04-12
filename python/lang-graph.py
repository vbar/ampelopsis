#!/usr/bin/python3

# requires database filled by running condensate.py

import csv
import json
import re
import sys
from common import get_option, make_connection
from known_names import KnownNames
from lang_wrap import init_lang_recog
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase
from token_util import tokenize

class Processor(PinholeBase):
    def __init__(self, cur, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        self.lang_recog = init_lang_recog()
        self.lang2total = {} # str lang -> int
        self.variant2langmap = {} # str hamlet name / int party id -> str lang -> int

    def dump_meta(self, output_path):
        meta = {
            'colors': self.make_meta(),
            'totals': self.lang2total,
            # old D3 in frontend doesn't parse ISO format...
            'dateExtent': [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]
        }

        with open(output_path, 'w') as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

    def dump_content(self, output_path):
        tail_keys = sorted(self.lang2total.keys(), key=lambda k: -1 * self.lang2total[k])
        with open(output_path, 'w') as f:
            writer = csv.writer(f, delimiter=",")
            headings = [ "name" ]
            headings.extend(tail_keys)
            writer.writerow(headings)
            for variant, langmap in self.variant2langmap.items():
                row = [ self.get_presentation_name(variant) ]
                for lng in tail_keys:
                    row.append(langmap.get(lng, 0))

                writer.writerow(row)

    def load_item(self, et):
        self.extend_date(et)
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
            langmap = { lng: 1 }
            self.variant2langmap[variant] = langmap
        else:
            cnt = langmap.get(lng, 0)
            langmap[lng] = cnt + 1

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

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()

                json_target = get_option("langbar_meta_data", "langbar.json")
                processor.dump_meta(json_target)
                csv_target = get_option("langbar_data", "langbar.csv")
                processor.dump_content(csv_target)
            finally:
                processor.close()


if __name__ == "__main__":
    main()
