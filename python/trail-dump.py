#!/usr/bin/python3

import sys
from common import make_connection
from page_frame import PageFrame

class Dumper(PageFrame):
    def __init__(self, cur, town_name):
        PageFrame.__init__(self, cur)
        self.heads = self.get_urls(town_name)

    def get_urls(self, town_name):
        mask = 'https://twitter.com/i/search/timeline?%&max_position=-1&q=from%%3A' + town_name + '+%'
        self.cur.execute("""select url, id
from field
where url like %s
order by id""", (mask,))
        return self.cur.fetchall()

    def dump(self):
        for url, url_id in self.heads:
            trail = self.get_trail(url, url_id)
            print(url, len(trail))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            for a in sys.argv[1:]:
                if a:
                    dumper = Dumper(cur, a.lower())
                    try:
                        dumper.dump()
                    finally:
                        dumper.close()


if __name__ == "__main__":
    main()
