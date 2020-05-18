#!/usr/bin/python3

# requires download with funnel_links set to at least 2 and database
# filled by running condensate.py

import collections
from datetime import datetime
import json
import locale
from lxml import etree
import pytz
import re
import sys
from common import make_connection
from party_mixin import PartyMixin
from show_case import ShowCase
from url_heads import short_town_url_head

profile_pattern = '^%s/([^/?#]+)$' % short_town_url_head

profile_rx = re.compile(profile_pattern)

ProfileDesc = collections.namedtuple('ProfileDesc', 'since following_count follower_count')


def by_follower_count(p):
    pd = p[1]
    major = 0 if pd.follower_count is None else pd.follower_count
    minor = 0 if pd.following_count is None else pd.following_count
    return (-1 * major, -1 * minor, p[0])


class Processor(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.html_parser = etree.HTMLParser()

        # not necessarily correct, but it's where the accounts should
        # have been created as well as where the client downloaded
        # from, and we have to use something...
        self.timezone = pytz.timezone('Europe/Prague')

        self.town_set = set()
        self.town2profile = {} # str town name -> ProfileDesc
        self.alt_person_map = {} # # str town name -> str presentation name
        self.init_identity()

    def init_identity(self):
        self.cur.execute("""select town_name, presentation_name
from vn_identity_town
join vn_record on record_id=id
order by town_name""")
        rows = self.cur.fetchall()
        for town_name, present_name in rows:
            self.alt_person_map[town_name] = present_name

    def run2(self):
        self.cur.execute("""select url, id
from field
where url ~ '%s'
order by url""" % re.sub("\\(\\)", "", profile_pattern))
        rows = self.cur.fetchall()
        for row in rows:
            self.load_profile(*row)

    def dump(self):
        name2color = {}
        profiles = []
        for town_name, profile in sorted(self.town2profile.items(), key=by_follower_count):
            hamlet_name = self.town2hamlet.get(town_name)
            if hamlet_name:
                party_name = None
                party_id = self.hamlet2party.get(hamlet_name)
                if party_id:
                    party_name = self.party_map[party_id]
                    if party_name not in name2color:
                        name2color[party_name] = self.convert_color(party_id)

                out = { 'name': self.person_map[hamlet_name] }
                if party_name:
                    out['party'] = party_name
            else:
                alt_name = self.alt_person_map.get(town_name, town_name)
                out = { 'name': alt_name }

            out['url'] = "%s/%s" % (short_town_url_head, town_name)
            out['core'] = town_name in self.town_set

            if profile.since:
                out['since'] = profile.since.isoformat()

            if profile.following_count is not None:
                out['following'] = profile.following_count

            if profile.follower_count is not None:
                out['followers'] = profile.follower_count

            profiles.append(out)

        custom = {
            'colors': name2color,
            'profiles': profiles,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        self.extend_date(et)
        hamlet_name = et['osobaid']
        town_name = self.hamlet2town.get(hamlet_name)
        if town_name:
            self.town_set.add(town_name)

    def load_profile(self, url, url_id):
        root = self.get_html_document(url_id)
        if not root:
            print("%s not found on disk" % url, file=sys.stderr)
            return

        town_name = self.get_town_name(url)
        self.town2profile[town_name] = ProfileDesc(
            self.get_since(root),
            self.get_count(root, 'following'),
            self.get_count(root, 'followers'))

    @staticmethod
    def get_town_name(url):
        m = profile_rx.match(url)
        return m.group(1)

    @staticmethod
    def get_count(root, nav):
        attrs = root.xpath("//a[@data-nav='%s']/span[@class='ProfileNav-value']/@data-count" % nav)
        count = None
        for a in attrs:
            try:
                c = int(a)
                if count is None:
                    count = c
                elif count != c:
                    raise Exception("profile has multiple %s counts" % nav)
            except:
                print("cannot parse count:", sys.exc_info()[0], file=sys.stderr)

        return count

    def get_since(self, root):
        attrs = root.xpath("//span[contains(@class, 'ProfileHeaderCard-joinDateText')]/@title")
        since = None
        for a in attrs:
            try:
                dt = datetime.strptime(a, "%I:%M %p - %d %b %Y")
                if since is None:
                    since = dt
                elif since != dt:
                    raise Exception("profile has multiple join dates")
            except:
                print("cannot parse date:", sys.exc_info()[0], file=sys.stderr)

        return self.timezone.localize(since)

    def get_html_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        try:
            return etree.parse(reader, self.html_parser)
        finally:
            reader.close()

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            locale.setlocale(locale.LC_ALL, "C") # to parse English dates
            processor = Processor(cur)
            try:
                processor.run()
                processor.run2()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
