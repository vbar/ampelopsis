#!/usr/bin/python3

import sys
from common import make_connection
from page_frame import PageFrame

class Dumper(PageFrame):
    def __init__(self, cur, town_names):
        PageFrame.__init__(self, cur)
        self.town_names = town_names

    def dump(self):
        for name in self.town_names:
            print(self.get_profile(name))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            names = [a.lower() for a in sys.argv[1:]]
            dumper = Dumper(cur, names)
            try:
                dumper.dump()
            finally:
                dumper.close()


if __name__ == "__main__":
    main()
