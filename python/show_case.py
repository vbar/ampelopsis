import sys
from page_frame import PageFrame

class ShowCase(PageFrame):
    def __init__(self, cur):
        PageFrame.__init__(self, cur)
        self.mindate = None
        self.maxdate = None
        self.town2items = {} # town name -> set of StatusItem

    def run(self):
        self.cur.execute("""select url, field.id, town_name
from field, panel_names
where url like concat('https://twitter.com/i/search/timeline?%&max_position=-1&q=from%%3A', town_name, '+%')
        and field.id not in ( select url_id from download_error )
        and checkd is not null
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.load_head(*row)

    def load_head(self, head_url, url_id, town_name):
        trail = self.get_trail(head_url, url_id)
        for si in trail:
            self.extend_date(si.dt)

        items = self.town2items.setdefault(town_name, set())
        items.update(trail)

    def get_status_items(self, town_name):
        items = self.town2items.get(town_name)
        if not items:
            return None

        return sorted(items, key=lambda si: (si.dt, si.url))

    def extend_date(self, dt):
        if (self.mindate is None) or (dt < self.mindate):
            self.mindate = dt

        if (self.maxdate is None) or (dt > self.maxdate):
            self.maxdate = dt
