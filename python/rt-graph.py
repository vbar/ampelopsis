#!/usr/bin/python3

# requires download with funnel_links set to at least 2

import json
import re
from urllib.parse import urlparse, urlunparse
from common import get_option, make_connection
from show_case import ShowCase

status_rx = re.compile("/([-\\w]+)/status/")

class Payload:
    def __init__(self, url):
        self.url_set = set()
        if url:
            self.url_set.add(url)

        self.count = 1

    def add(self, url):
        if url:
            self.url_set.add(url)

        self.count += 1


class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.panel_set = set()
        self.key2payload = {}
        self.get_url_key = self.get_profile_key if get_option("profile_url_key", "1") else self.get_identity_key

    def get_profile_key(self, url):
        pr = urlparse(url)
        m = status_rx.match(pr.path)
        if not m:
            return None

        town_name = m.group(1)
        profile_pr = (pr.scheme, pr.netloc, town_name, '', '', '')
        return urlunparse(profile_pr)

    def get_identity_key(self, url):
        return url

    def dump(self):
        profiles = []
        for url, payload in sorted(self.key2payload.items(), key=lambda p: (-1 * p[1].count, p[0])):
            profiles.append([url, payload.count, len(payload.url_set)])

        custom = {
            'panelCount': len(self.panel_set),
            'profiles': profiles,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        self.extend_date(et)
        panel_url = et.get('url')
        if panel_url:
            panel_key = self.get_url_key(panel_url)
            if panel_key:
                self.panel_set.add(panel_key)

            self.cur.execute("""select f2.url
from field f1
join redirect on f1.id=from_id
join field f2 on to_id=f2.id
where f1.url=%s""", (panel_url,))
            rows = self.cur.fetchall()
            for row in rows:
                self.add_redirect(panel_key, row[0])

    def add_redirect(self, panel_key, orig_url):
        orig_key = self.get_url_key(orig_url)
        if orig_key:
            payload = self.key2payload.get(orig_key)
            if payload is None:
                self.key2payload[orig_key] = Payload(panel_key)
            else:
                payload.add(panel_key)

    def make_date_extent(self):
        return [dt.strftime("%Y-%m-%dT%H:%M:%S") for dt in (self.mindate, self.maxdate)]


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
