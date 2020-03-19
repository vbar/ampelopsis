#!/usr/bin/python3

import datetime
from dateutil.parser import parse
import matplotlib.pyplot as plt
import sys
from common import make_connection
from show_case import ShowCase

class Timeline(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.mindate = None
        self.maxdate = None
        self.timeline = []

    def get_timeline(self):
        return sorted(self.timeline)

    def dump_range(self):
        if (self.mindate is None) or (self.maxdate is None):
            return

        print("%s - %s" % (self.mindate, self.maxdate))

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            pdt = parse(et.get('datum'))
            dt = pdt.replace(microsecond=0, second=0, minute=0)
            if (self.mindate is None) or (self.mindate > dt):
                self.mindate = dt

            if (self.maxdate is None) or (self.maxdate < dt):
                self.maxdate = dt

            self.timeline.append(dt)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            builder = Timeline(cur)
            try:
                builder.run()
                timeline = builder.get_timeline()
                l = len(timeline)
                if l:
                    builder.dump_range()
                    delta = datetime.timedelta(hours=1)
                    xseries = []
                    yseries = []
                    idx = 0
                    dt = timeline[0]
                    maxdt = timeline[-1]
                    freq = 0
                    while dt <= maxdt:
                        while (idx < l) and (dt == timeline[idx]):
                            freq += 1
                            idx += 1

                        xseries.append(dt)
                        yseries.append(freq)
                        dt += delta
                        freq = 0

                    plt.plot(xseries, yseries)
                    plt.show()
            finally:
                builder.close()


if __name__ == "__main__":
    main()
