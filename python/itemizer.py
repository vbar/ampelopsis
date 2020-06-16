from datetime import datetime
from io import StringIO
from lxml import etree
import pytz
from urllib.parse import urljoin

class Itemizer:
    def __init__(self):
        self.html_parser = etree.HTMLParser()

        # not necessarily correct, but it's where the accounts should
        # have been created as well as where the client downloaded
        # from, and we have to use something...
        self.timezone = pytz.timezone('Europe/Prague')

    def split_items(self, page_items):
        html = """<!DOCTYPE html>
<html><body><ol>
%s
</ol></body></html>""" % page_items
        root = etree.parse(StringIO(html), self.html_parser)
        return root.xpath("//li[starts-with(@class, 'js-stream-item ')]")

    def get_item_url(self, page_url, node):
        path_attrs = node.xpath("div/@data-permalink-path")
        l = len(path_attrs)
        if l != 1:
            raise Exception("%d permalinks in %s" % (l, page_url))

        return urljoin(page_url, path_attrs[0])

    def get_item_time(self, node):
        # doesn't agree w/ string representation in title, but title
        # is incorrect...
        time_attrs = node.xpath(".//a[starts-with(@class, 'tweet-timestamp ')]/span/@data-time")
        l = len(time_attrs)
        if l != 1:
            raise Exception("%d item times" % l)

        time_attr = time_attrs[0]
        dt = datetime.fromtimestamp(int(time_attr))
        return self.timezone.localize(dt)

    def is_retweet(self, node):
        rt_nodes = node.xpath(".//span[@class='js-retweet-text']")
        return len(rt_nodes) > 0
