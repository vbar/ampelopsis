#!/usr/bin/python3

import collections
from datetime import datetime
import json
import re
import sys
from common import get_option, make_connection
from show_case import ShowCase

profile_rx = re.compile('^https://mobile.twitter.com/([^/?#]+)$')


def by_follower_count(p):
    pd = p[1]
    major = 0 if pd.followers is None else pd.followers
    minor = 0 if pd.following is None else pd.following
    return (-1 * major, -1 * minor, p[0])


class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.town2profile = {}

    def run2(self):
        self.cur.execute("""select url, id
from field
where url ~ '^https://mobile.twitter.com/[^/?#]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.load_profile(*row)

    def dump(self):
        profiles = []
        for town_name, profile in sorted(self.town2profile.items(), key=by_follower_count):
            out = { 'name': profile.name or "???" }

            out['url'] = "https://twitter.com/" + town_name
            out['core'] = town_name in self.town2items

            if profile.since:
                out['since'] = profile.since.isoformat()

            if profile.following is not None:
                out['following'] = profile.following

            if profile.followers is not None:
                out['followers'] = profile.followers

            profiles.append(out)

        custom = {
            'profiles': profiles,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_profile(self, url, url_id):
        town_name = self.get_town_name(url)
        print("loading %s..." % town_name, file=sys.stderr)
        self.town2profile[town_name] = self.get_profile(town_name)

    @staticmethod
    def get_town_name(url):
        m = profile_rx.match(url)
        return m.group(1)

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.run2()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
