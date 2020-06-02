from lxml import etree
import re
import sys
from urllib.parse import parse_qs, urljoin, urlparse
from url_heads import alt_town_url_head, short_town_url_head

rel_town_rx = re.compile("^/([a-zA-Z0-9_./-]+)")

class TrailMixin:
    def __init__(self):
        self.html_parser = etree.HTMLParser()

    def make_followers_set(self, town_name, include_trail=False):
        def make_return_value():
            primary = frozenset(town_set)
            return (primary, trail) if include_trail else primary

        town_set = set()
        trail = None
        url = "%s/%s/followers" % (alt_town_url_head, town_name)
        while url:
            if include_trail:
                if trail is None:
                    trail = []
                else:
                    cursor = self.get_cursor(url)
                    if cursor is None:
                        print("URL " + url + " has no cursor", file=sys.stderr)
                    else:
                        trail.append(cursor)

            url_id = self.get_url_id(url)
            if not url_id:
                return make_return_value()

            root = self.get_html_document(url_id)
            if not root:
                print("cannot parse: " + url, file=sys.stderr)
                return make_return_value()

            town_set = town_set | self.get_followers_set(root)
            url = self.get_followers_next(url, root)

        return make_return_value()

    def get_follower_count(self, town_name):
        url = "%s/%s" % (short_town_url_head, town_name)
        url_id = self.get_url_id(url)
        if not url_id:
            return None

        root = self.get_html_document(url_id)
        if not root:
            print("no profile for " + town_name, file=sys.stderr)
            return None

        return self.get_profile_count(root, 'followers')

    @staticmethod
    def get_profile_count(root, nav):
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

    @staticmethod
    def get_cursor(url):
        pr = urlparse(url)
        params = parse_qs(pr.query)
        vals = params['cursor']
        if not vals:
            return None

        try:
            return int(vals[0])
        except:
            return None

    @staticmethod
    def get_followers_set(root):
        page_set = set()
        attrs = root.xpath("//a[@data-scribe-action='profile_click']/@href")
        for a in attrs:
            m = rel_town_rx.match(a)
            if m:
                name = m.group(1)
                page_set.add(name.lower())

        return page_set

    @staticmethod
    def get_followers_next(base_url, root):
        next_url = None
        attrs = root.xpath("//div[@class='w-button-more']/a/@href")
        for a in attrs:
            nu = urljoin(base_url, a)
            if next_url is None:
                next_url = nu
            elif next_url != nu:
                raise Exception("%s has multiple more buttons" % base_url)

        return next_url

    def get_html_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        try:
            return etree.parse(reader, self.html_parser)
        finally:
            reader.close()
