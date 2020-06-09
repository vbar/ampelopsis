import collections
from datetime import datetime
from io import StringIO
import json
from lxml import etree
import pytz
import sys
from urllib.parse import urljoin
from cursor_wrapper import CursorWrapper
from trail_util import get_next_url
from volume_holder import VolumeHolder


StatusItem = collections.namedtuple('StatusItem', 'url dt rt')


class Page:
    def __init__(self, base_url, cursor_next):
        self.base_url = base_url
        self.cursor_next = cursor_next
        self.items = [] # of StatusItem


class PageFrame(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.html_parser = etree.HTMLParser()

        # not necessarily correct, but it's where the accounts should
        # have been created as well as where the client downloaded
        # from, and we have to use something...
        self.timezone = pytz.timezone('Europe/Prague')

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

    def parse_page(self, url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            return None

        page = Page(url, doc.get('min_position'))
        items = doc.get('items_html')
        html = """<!DOCTYPE html>
<html><body><ol>
%s
</ol></body></html>""" % items
        root = etree.parse(StringIO(html), self.html_parser)

        nodes = root.xpath("//li[starts-with(@class, 'js-stream-item ')]")
        for node in nodes:
            path_attrs = node.xpath("div/@data-permalink-path")
            if len(path_attrs) == 1:
                path_attr = path_attrs[0]
            else:
                raise Exception("%d permalinks in %s" % (len(path_attrs), url))

            # doesn't agree w/ string representation in title, but
            # title is incorrect...
            time_attrs = node.xpath(".//a[starts-with(@class, 'tweet-timestamp ')]/span/@data-time")
            if len(time_attrs) == 1:
                time_attr = time_attrs[0]
            else:
                raise Exception("%d times in %s" % (len(time_attrs), url))

            rt_nodes = node.xpath(".//span[@class='js-retweet-text']")

            dt = datetime.fromtimestamp(int(time_attr))
            si = StatusItem(urljoin(url, path_attr), self.timezone.localize(dt), len(rt_nodes) > 0)
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
