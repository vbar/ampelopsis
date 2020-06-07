import json
import re
import sys
from trail_util import get_next_url

class TrailParser:
    def __init__(self, owner, url):
        self.owner = owner
        self.base = url

    def parse_links(self, fp):
        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))
        cursor = doc.get('min_position')
        if cursor:
            next_url = get_next_url(self.base, cursor)
            if next_url:
                self.owner.add_link(next_url)
