from json_frame import JsonFrame
from url_heads import hamlet_url_head
from volume_holder import VolumeHolder

class ShowCase(JsonFrame):
    def __init__(self, cur):
        JsonFrame.__init__(self, cur)

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '^%s'
and checkd is not null
and url_id is null
order by url""" % hamlet_url_head)
        rows = self.cur.fetchall()
        for row in rows:
            self.load_page(*row)
