#!/usr/bin/python3

import sys
from urllib.parse import urlparse, urlunparse
from common import get_netloc, make_connection, normalize_url_component
from host_check import HostCheck
from page_parser import PageParser
from volume_holder import VolumeHolder

class Builder(VolumeHolder, HostCheck):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        HostCheck.__init__(self, cur)
        self.children = None

    def add(self, url, url_id):
        print("adding " + url + "...", file=sys.stderr)

        volume_id = self.get_volume_id(url_id)
        f = self.open_page(url_id, volume_id)
        if f is not None:
            self.parse_file(url, f)
            self.insert_links(url_id)
        else:
            self.cond_insert_redir(url_id)

    def parse_file(self, url, page_file):
        try:
            self.children = set()
            parser = PageParser(self, url)
            parser.parse_links(page_file)
        finally:
            page_file.close()

    def add_link(self, url):
        pr = urlparse(url.strip())
        if pr.hostname: # may not exist even for valid links, e.g. mailto:
            host_id = self.get_host_id(pr.hostname)
            if host_id:
                clean_pr = (pr.scheme, get_netloc(pr), normalize_url_component(pr.path), pr.params, normalize_url_component(pr.query), '')
                clean_url = urlunparse(clean_pr)
                child_url_id = self.get_url_id(clean_url)
                if child_url_id:
                    self.children.add(child_url_id)

    def insert_links(self, parent_url_id):
        for child_url_id in self.children:
            if parent_url_id != child_url_id:
                self.cur.execute("""insert into edges(from_id, to_id)
values(%s, %s)
on conflict do nothing""", (parent_url_id, child_url_id))

            self.cur.execute("""insert into nodes(url_id)
values(%s)
on conflict do nothing""", (child_url_id,))

    def cond_insert_redir(self, source_id):
        self.cur.execute("""select to_id
from redirect
where from_id=%s""", (source_id,))
        rows = self.cur.fetchall()
        if len(rows) > 1:
            raise Exception(str(source_id) + " redirects to multiple targets")

        if len(rows) == 1:
            row = rows[0]
            target_id = row[0]
            self.cur.execute("""insert into edges(from_id, to_id)
values(%s, %s)
on conflict do nothing""", (source_id, target_id))

            self.cur.execute("""insert into nodes(url_id)
values(%s)
on conflict do nothing""", (target_id,))

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        return row[0] if row else None


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            builder = Builder(cur)
            try:
                cur.execute("""select url, id
from field
left join nodes on id=url_id
where checkd is not null and (url_id is null or depth=0)
order by url""")
                rows = cur.fetchall()
                for row in rows:
                    builder.add(*row)
            finally:
                builder.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
