import json
import re
import sys
from query_format import format_home, format_quarry
from urllib.parse import urlparse
from trail_util import get_next_url

class TrailParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.base = url
        self.match = re.match("^https://twitter.com/i/search/timeline", url)

    def parse_links(self, fp):
        if not self.match:
            # do not parse home pages
            return

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))
        cursor = doc.get('min_position')
        if cursor:
            next_url = get_next_url(self.base, cursor)
            if next_url:
                self.owner.add_link(next_url)

        items = doc.get('items_html')
        nodes = self.owner.itemizer.split_items(items)
        for node in nodes:
            url = self.owner.itemizer.get_item_url(self.base, node)
            pr = urlparse(url)
            segments = pr.path.split('/')
            if len(segments) > 1:
                raw_name = segments[1]
                if raw_name:
                    town_name = raw_name.lower()
                    self.owner.add_link(format_home(town_name))
                    self.owner.add_link(format_quarry(town_name))
