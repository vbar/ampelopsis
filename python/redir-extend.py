#!/usr/bin/python3

# requires download with funnel_links set (to at least 1)

from lxml import etree
import re
import sys
from common import make_connection
from cursor_wrapper import CursorWrapper
from url_heads import short_town_url_head
from volume_holder import VolumeHolder

status_path = "/[^/]+/status/[0-9]+$"

class Extender(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.html_parser = etree.HTMLParser()
        self.date_rx = re.compile("^[0-9]{1,2}:[0-9]{2}.+ [0-9]{4}$")
        self.path_rx = re.compile("^" + status_path)

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s%s'
and checkd is not null
and url_id is null
order by url""" % (short_town_url_head, status_path))
        rows = self.cur.fetchall()
        for row in rows:
            self.ensure_redir(*row)

    def ensure_redir(self, source_url, source_url_id):
        print("checking %s..." % (source_url,), file=sys.stderr)

        root = self.get_html_document(source_url_id)
        if not root:
            return

        target_url = self.get_redir_target(root)
        if not target_url:
            return

        target_url_id = self.get_url_id(target_url)
        if not target_url_id:
            print("adding %s..." % (target_url,), file=sys.stderr)

            # if seed is run after this, the extra pages will actually
            # be scheduled for download - is that desirable?
            self.cur.execute("""insert into field(url)
values(%s)
returning id""", (target_url,))
            row = self.cur.fetchone()
            target_url_id = row[0]

        if source_url_id != target_url_id:
            self.cur.execute("""insert into redirect(from_id, to_id)
values(%s, %s)
on conflict do nothing""", (source_url_id, target_url_id))

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            return None

        return row[0]

    def get_html_document(self, url_id):
        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print("page not found", file=sys.stderr)
            return None

        try:
            return etree.parse(reader, self.html_parser)
        finally:
            reader.close()

    def get_redir_target(self, root):
        anchors = root.xpath("//article//a")
        if not self.has_retweet_text(anchors):
            return None

        for a in anchors:
            if self.has_date_text(a):
                href = a.get('href')
                if href and self.path_rx.match(href):
                    return short_town_url_head + href

        return None

    def has_date_text(self, a):
        for t in a.xpath(".//text()"):
            if self.date_rx.match(str(t)):
                return True

        return False

    def has_retweet_text(self, anchors):
        for a in anchors:
            for t in a.xpath(".//text()"):
                if t.endswith("Retweeted"):
                    return True

        return False


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            ext = Extender(cur)
            ext.run()


if __name__ == "__main__":
    main()
