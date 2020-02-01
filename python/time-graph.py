#!/usr/bin/python3

import sys
from matplotlib.patches import Rectangle
import matplotlib.pyplot as plt
import numpy as np
from common import get_option, make_connection, schema
from json_lookup import JsonLookup
from schema_manager import SchemaManager

flag_legend = ("generic", "generic failure", "specific", "specific failure")

def get_flags(specific_flag, error):
    f = 0 if specific_flag else 2
    if error is not None:
        f += 1

    return f


class Scanner(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)
        self.doc_count = 0
        self.query_count = 0
        self.times = [] # of (time, flags)

    def run(self):
        self.cur.execute("""select url
from field
left join download_error on id=url_id
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
and checkd is not null
and url_id is null
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.scan(row[0])

    def get_times(self):
        return sorted(self.times)

    def scan(self, url):
        self.doc_count += 1
        detail = self.get_document(url)
        if not detail:
            print(url + " not found", file=sys.stderr)
            return

        generic_url = self.make_query_single_url(detail, set())
        self.process(generic_url, False)

        position_set = self.make_position_set(detail)
        l = len(position_set)
        if l:
            specific_urls = self.make_query_urls(detail, position_set)
            for specific_url in specific_urls:
                self.process(specific_url, True)

    def process(self, url, specific_flag):
        self.query_count += 1
        if not (self.query_count % 1000):
            print("%d queries from %d documents" %
                  (self.query_count, self.doc_count), file=sys.stderr)

        self.cur.execute("""select id, error_code
from field
left join download_error on id=url_id
where url=%s""", (url,))
        row = self.cur.fetchone()
        if not row:
            print("URL %s not found" % url)
            return

        url_id = row[0]
        error = row[1]
        volume_id = self.get_volume_id(url_id)
        t = self.get_time(url_id, volume_id)
        if t is not None:
            self.times.append((t, get_flags(specific_flag, error)))

    def get_time(self, url_id, volume_id):
        raw_time = None
        f = self.open_headers(url_id, volume_id)
        if f is not None:
            try:
                for ln in f:
                    header_line = ln.decode('iso-8859-1')
                    line_list = header_line.split(':', 1)
                    if len(line_list) == 2:
                        name, value = line_list
                        name = name.strip()
                        name = name.lower()
                        if name == 'x-envoy-upstream-service-time':
                            if raw_time is not None:
                                # could somehow aggregate here, but
                                # since this case probably won't
                                # happen...
                                raise Exception("time repeated")

                            raw_time = value.strip()
            finally:
                f.close()

        if raw_time is None:
            return None

        return int(raw_time)


def get_times(cur):
    scanner = Scanner(cur)
    scanner.run()
    times = scanner.get_times()
    print("%d timed URLs found" % len(times), file=sys.stderr)
    return times


def plot_times(title, marker, times):
    series = [] # of list of int
    for i in range(2 * len(flag_legend)): # x- & y- series for every segment
        series.append([])

    for i, tpl in enumerate(times):
        idx = 2 * tpl[1]
        xseries = series[idx]
        yseries = series[idx + 1]
        xseries.append(i)
        yseries.append(tpl[0])

    segments = []
    colors = ('g', 'r', 'b', 'm')
    assert len(colors) == len(flag_legend)
    for i in range(len(flag_legend)):
        idx = 2 * i
        ln = plt.scatter(series[idx], series[idx + 1], marker=marker, color=colors[i])
        segments.append(ln)

    return segments


def main():
    time_data = [] # of (title, marker, times)
    with make_connection() as conn:
        with conn.cursor() as cur:
            old_schema = get_option("old_schema", None)
            if old_schema:
                with SchemaManager(old_schema, cur):
                    time_data.append((old_schema, 'x', get_times(cur)))

            time_data.append((schema, '.', get_times(cur)))

    # legend based on https://stackoverflow.com/questions/24787041/multiple-titles-in-legend-in-matplotlib
    handles = []
    labels = []
    title_proxy = Rectangle((0,0), 0, 0, color='w')
    for p in time_data:
        segments = plot_times(*p)
        assert len(segments) == len(flag_legend)
        handles.append(title_proxy)
        labels.append("$%s$" % p[0])
        for i in range(len(flag_legend)):
            handles.append(segments[i])
            labels.append(flag_legend[i])

    plt.legend(handles, labels)

    plt.show()

if __name__ == "__main__":
    main()
