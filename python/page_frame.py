import collections
from datetime import datetime
import json
from lxml import etree
import sys
from cursor_wrapper import CursorWrapper
from systemizer import Systemizer
from query_format import format_home, format_quarry
from trail_util import get_next_url
from volume_holder import VolumeHolder

StatusItem = collections.namedtuple('StatusItem', 'url dt rt')

ProfileDesc = collections.namedtuple('ProfileDesc', 'name since following followers')

class Page:
    def __init__(self, base_url, cursor_next):
        self.base_url = base_url
        self.cursor_next = cursor_next
        self.items = [] # of StatusItem


class PageFrame(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.systemizer = Systemizer()

    def get_profile(self, town_name):
        url = format_home(town_name)
        url_id = self.get_url_id(url)
        if not url_id:
            return None

        root = self.get_html_document(url_id)
        if not root:
            return None

        name = self.systemizer.get_profile_name(root)
        since = self.get_since(town_name)
        following = self.systemizer.get_profile_following(root)
        followers = self.systemizer.get_profile_followers(root)
        return ProfileDesc(name, since, following, followers)

    def get_trail(self, url, url_id):
        trail = []
        page = self.parse_page(url, url_id)
        while page:
            # print("got " + page.base_url, file=sys.stderr)
            trail.extend(page.items)
            next_url = get_next_url(page.base_url, page.cursor_next)
            if next_url and (next_url != page.base_url):
                next_id = self.get_url_id(next_url)
                if next_id:
                    page = self.parse_page(next_url, next_id)
                else:
                    page = None
            else:
                page = None

        return trail

    def get_since(self, town_name):
        url = format_quarry(town_name)
        url_id = self.get_url_id(url)
        if not url_id:
            return None

        root = self.get_html_document(url_id)
        if not root:
            return None

        return self.systemizer.get_quarry_since(root)

    def parse_page(self, url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            return None

        page = Page(url, doc.get('min_position'))
        items = doc.get('items_html')
        nodes = self.systemizer.split_items(items)
        for node in nodes:
            item_url = self.systemizer.get_item_url(url, node)
            item_dt = self.systemizer.get_item_time(node)
            rt_flag = self.systemizer.is_retweet(node)
            si = StatusItem(item_url, item_dt, rt_flag)
            page.items.append(si)

        return page

    def get_url_id(self, url):
        self.cur.execute("""select id, checkd
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            print("unknown URL " + url, file=sys.stderr)
            return None

        if row[1] is None:
            print("URL " + url + " not downloaded", file=sys.stderr)
            return None

        return row[0]

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

    def get_html_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        try:
            return etree.parse(reader, self.systemizer.html_parser)
        finally:
            reader.close()
