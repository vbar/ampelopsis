#!/usr/bin/python3

import datetime
from dateutil.parser import parse
import json
import matplotlib.pyplot as plt
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class Timeline(VolumeHolder, CursorWrapper):
    hamlet_url_head = "https://www.hlidacstatu.cz/api/v1/DatasetSearch/vyjadreni-politiku"

    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.mindate = None
        self.maxdate = None
        self.timeline = []

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s'
and checkd is not null
and url_id is null
order by url""" % self.hamlet_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.load_page(*row)

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

    def get_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        buf = b''
        try:
            for ln in reader:
                buf += ln
        finally:
            reader.close()

        return json.loads(buf.decode('utf-8'))


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
