#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

from common import make_connection
from distance_args import ConfigArgs
from rain_processor import RainProcessor
from stop_util import load_stop_words

def main():
    ca = ConfigArgs()
    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = RainProcessor(cur, stop_words)
            try:
                processor.run()
                processor.process()
                processor.dump_undirected()
                if ca.histogram:
                    processor.dump_distance_histogram(ca.histogram)
            finally:
                processor.close()


if __name__ == "__main__":
    main()
