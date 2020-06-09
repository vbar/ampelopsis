#!/usr/bin/python3

import csv
import datetime
import sys
from common import make_connection
from show_case import ShowCase

class Timeline(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.timeline = []
        self.redir_timeline = []

    def process(self):
        for town_name, items in self.town2items.items():
            for si in items:
                dt = si.dt.replace(microsecond=0, second=0, minute=0)
                self.timeline.append(dt)

                if si.rt:
                    self.redir_timeline.append(dt)

    def get_timeline(self):
        return sorted(self.timeline)

    def get_redir_timeline(self):
        return sorted(self.redir_timeline)

    def dump_range(self):
        if (self.mindate is None) or (self.maxdate is None):
            return

        print("%s - %s" % (self.mindate, self.maxdate), file=sys.stderr)


def make_series(timeline):
    l = len(timeline)
    assert l

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

    return series


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            builder = Timeline(cur)
            try:
                builder.run()
                builder.process()
                timeline = builder.get_timeline()
                l = len(timeline)
                if l:
                    builder.dump_range()
                    series = make_series(timeline)

                    redir_map = None
                    redir_timeline = builder.get_redir_timeline()
                    if len(redir_timeline):
                        redir_map = {}
                        redir_series = make_series(redir_timeline)
                        for point in redir_series:
                            redir_map[point[0]] = point[1]

                    writer = csv.writer(sys.stdout, delimiter=",")
                    headings = ['date', "value"]
                    if redir_map:
                        headings.append("redirOnly")

                    writer.writerow(headings)
                    for point in series:
                        row = []
                        row.extend(point)
                        if redir_map:
                            row.append(redir_map.get(point[0], 0))

                        writer.writerow(row)
            finally:
                builder.close()


if __name__ == "__main__":
    main()
