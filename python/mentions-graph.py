#!/usr/bin/python3

# requires database filled by running condensate.py

from common import make_connection
from opt_util import get_quoted_list_option
from term_freq_base import TermFreqBase

class Processor(TermFreqBase):
    def __init__(self, cur, deconstructed):
        TermFreqBase.__init__(self, cur, '(@[-\\w]+)', deconstructed)


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
