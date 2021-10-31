#!/usr/bin/python3

# requires database filled by running condensate.py

import json
import math
import numpy as np
import random
import sys
from common import get_option, make_connection
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase
from token_util import tokenize

class Processor(PinholeBase):
    def __init__(self, cur, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        self.variant2length = {} # variant => list of int lengths
        self.data = []

    def load_item(self, rec):
        url = self.get_circuit_url(rec)

        hamlet_name = rec['OsobaId']
        variant = self.get_variant(hamlet_name)
        if not variant:
            return

        lst = tokenize(rec['text'])
        length = len(lst)
        if not length: # empty text won't fit log scale
            return

        self.extend_date(rec)

        payload = self.variant2length.get(variant)
        if payload is None:
            self.variant2length[variant] = [ length ]
        else:
            payload.append(length)

        item = ( url, variant, length )
        self.data.append(item)

    def dump(self):
        keys = [ p[0] for p in sorted(self.variant2length.items(), key=lambda q: (len(q[1]), str(q[0]))) ]
        indirect = {}
        names = []
        colors = []
        sampling = {} # variant => fraction
        nest = []
        for idx, variant in enumerate(keys):
            present_name = self.get_presentation_name(variant)
            var_color = self.introduce_color(variant)
            indirect[variant] = idx
            names.append(present_name)
            colors.append(var_color)

            payload = self.variant2length[variant]
            full_count = len(payload)
            if full_count == 1:
                sample_target = 1
            else:
                sample_target = math.log2(full_count)

            sampling[variant] = sample_target / full_count

            sorted_payload = np.array(sorted(payload))
            q1 = np.quantile(sorted_payload, 0.25)
            median = np.quantile(sorted_payload, 0.5)
            q3 = np.quantile(sorted_payload, 0.75)
            iqr = q3 - q1
            value_obj = {
                'color': var_color,
                'interQuantileRange': iqr,
                'max': int(sorted_payload[-1]),
                'median': median,
                'min': int(sorted_payload[0]),
                'q1': q1,
                'q3': q3
            }

            kv = {
                'key': present_name,
                'value': value_obj
            }

            nest.append(kv)

        data = []
        for url, variant, length in self.data:
            if random.random() <= sampling[variant]:
                item = [ url, indirect[variant], length ]
                data.append(item)

        custom = {
            'names': names,
            'colors': colors,
            'sumstat': nest,
            'data': data
        }

        if self.mindate:
            custom['dateExtent'] = self.make_date_extent()

        json.dump(custom, sys.stdout, indent=2, ensure_ascii=False)

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]


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
