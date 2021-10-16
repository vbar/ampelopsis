import json
import sys
from cursor_wrapper import CursorWrapper
from common import get_option
from volume_holder import VolumeHolder
from url_heads import hamlet_record_head

class JsonFrame(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.short_circuit_template = get_option("short_circuit_template", None)

    def run(self):
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url like '%s%s'
and checkd is not null
and url_id is null
order by url""" % (hamlet_record_head, '%'))
        rows = self.cur.fetchall()
        for row in rows:
            self.load_page(*row)

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            print("unknown URL " + url, file=sys.stderr)
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

    def is_redirected(self, url):
        self.cur.execute("""select count(*)
from field
join redirect on id=from_id
where url=%s""", (url,))
        row = self.cur.fetchone()
        return row[0] > 0

    def get_circuit_url(self, url):
        if not self.short_circuit_template:
            return url
        else:
            url_id = self.get_url_id(url)
            if url_id is None:
                return url
            else:
                return self.short_circuit_template.format(url_id)

    def ensure_url_id(self, url):
        # simpler than download updates because it isn't safe for
        # parallel instances
        self.cur.execute("""insert into field(url, checkd, parsed)
values(%s, localtimestamp, localtimestamp)
on conflict(url) do nothing
returning id""", (url,))
        row = self.cur.fetchone()
        if row:
            return row[0]

        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        return row[0]
