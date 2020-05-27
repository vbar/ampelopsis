from lxml import etree
import re
import sys
from urllib.parse import urljoin
from url_heads import alt_town_url_head

rel_town_rx = re.compile("^/([a-zA-Z0-9_./-]+)")

class TrailMixin:
    def __init__(self):
        self.html_parser = etree.HTMLParser()

    def make_followers_set(self, town_name):
        town_set = set()
        url = "%s/%s/followers" % (alt_town_url_head, town_name)
        while url:
            url_id = self.get_url_id(url)
            if not url_id:
                return town_set

            root = self.get_html_document(url_id)
            if not root:
                print("cannot parse: " + url, file=sys.stderr)
                return town_set

            town_set = town_set | self.get_followers_set(root)
            url = self.get_followers_next(url, root)

        return frozenset(town_set)

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
