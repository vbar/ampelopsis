#!/usr/bin/python3

import datetime
from dateutil.parser import parse
import sys
from common import make_connection
from line_output import ConfigLineOutput
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

        print("%s - %s" % (self.mindate, self.maxdate), file=sys.stderr)

    def load_item(self, et):
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
                    series = []
                    idx = 0
                    dt = timeline[0]
                    maxdt = timeline[-1]
                    freq = 0
                    while dt <= maxdt:
                        while (idx < l) and (dt == timeline[idx]):
                            freq += 1
                            idx += 1

                        series.append((dt, freq))
                        dt += delta
                        freq = 0

                    output = ConfigLineOutput(series)
                    output.output()
            finally:
                builder.close()


if __name__ == "__main__":
    main()
