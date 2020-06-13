#!/usr/bin/python3

import csv
from datetime import datetime
from dateutil.parser import parse
import sys
from common import make_connection
from show_case import ShowCase

class Dumper(ShowCase):
    def __init__(self, cur, day):
        ShowCase.__init__(self, cur)
        self.day = day
        self.occurences = [] # of StatusItem

    def process(self):
        for town_name, items in self.town2items.items():
            for si in items:
                pdt = si.dt
                if (pdt.year == self.day.year) and (pdt.month == self.day.month) and (pdt.day == self.day.day):
                    self.occurences.append(si)

    def dump(self):
        writer = csv.writer(sys.stdout, delimiter="\t")
        self.occurences.sort(key=lambda occ: occ.dt)
        for occ in self.occurences:
            row = ( occ.url, occ.dt )
            writer.writerow(row)


def main():
    if len(sys.argv) != 2:
        raise Exception("usage: " + sys.argv[0] + " yyyy-mm-dd")

    day = datetime.strptime(sys.argv[1], "%Y-%m-%d")

    with make_connection() as conn:
        with conn.cursor() as cur:
            dumper = Dumper(cur, day)
            try:
                dumper.run()
                dumper.process()
                dumper.dump()
            finally:
                dumper.close()


if __name__ == "__main__":
    main()
