#!/usr/bin/python3

from common import make_connection
from length_base import LengthBase
from token_util import tokenize

class Processor(LengthBase):
    def __init__(self, cur):
        LengthBase.__init__(self, cur)

    def get_length(self, att):
        txt = att.get('DocumentPlainText')
        if not txt:
            return 0

        lst = tokenize(txt)
        return len(lst)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            processor.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
