#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import json
import numpy as np
import sys
from common import get_option, make_connection
from rain_processor import RainProcessor
from stop_util import load_stop_words

def run(cur):
    stop_words = load_stop_words()

    hyper_repeat = int(get_option("hyper_repeat", "1"))
    cluster_count_bottom = int(get_option("cluster_count_bottom", "8"))
    cluster_count_top = int(get_option("cluster_count_top", "2048"))
    cluster_count_stride = int(get_option("cluster_count_stride", "10"))

    processor = RainProcessor(cur, stop_words)
    try:
        processor.run()

        data = []
        variance = []
        for idx in range(hyper_repeat):
            line = []
            for cluster_count in range(cluster_count_bottom, cluster_count_top + 1, cluster_count_stride):
                processor.cluster_count = cluster_count
                processor.value_series = None
                processor.process()
                gap = processor.get_distance_gap()
                print("round %d of %d: %d -> %f" % (idx + 1, hyper_repeat, cluster_count, gap), file=sys.stderr)

                line.append([cluster_count, gap])

            data.append(line)

            liney = [ it[1] for it in line ]
            variance.append(np.var(liney))

        custom = {
            'data': data,
            'variance': variance
        }

        if processor.mindate and processor.maxdate:
            custom['dateExtent'] = processor.make_date_extent()

        print(json.dumps(custom, indent=2))
    finally:
        processor.close()


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            run(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
