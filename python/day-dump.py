#!/usr/bin/python3

import collections
import csv
from datetime import datetime
from dateutil.parser import parse
import sys
from common import make_connection
from show_case import ShowCase

DayOcc = collections.namedtuple('DayOcc', 'url dt')

class Dumper(ShowCase):
    def __init__(self, cur, day):
        ShowCase.__init__(self, cur)
        self.day = day
        self.occurences = [] # of DayOcc

    def dump(self):
        writer = csv.writer(sys.stdout, delimiter="\t")
        self.occurences.sort(key=lambda occ: occ.dt)
        for occ in self.occurences:
            row = ( occ.url, occ.dt )
            writer.writerow(row)

    def load_item(self, et):
        pdt = parse(et['datum'])
        if (pdt.year == self.day.year) and (pdt.month == self.day.month) and (pdt.day == self.day.day):
            occ = DayOcc(et['url'], pdt)
            self.occurences.append(occ)


def main():
    if len(sys.argv) != 2:
        raise Exception("usage: " + sys.argv[0] + " yyyy-mm-dd")

    day = datetime.strptime(sys.argv[1], "%Y-%m-%d")

    with make_connection() as conn:
        with conn.cursor() as cur:
            dumper = Dumper(cur, day)
            try:
                dumper.run()
                dumper.dump()
            finally:
                dumper.close()


if __name__ == "__main__":
    main()
