import json
import sys
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class JsonFrame(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

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

    def get_redirect_target(self, raw_url_id):
        self.cur.execute("""select to_id
from redirect
where from_id=%s""", (raw_url_id,))
        rows = self.cur.fetchall()
        url_id = raw_url_id
        first = True
        for row in rows:
            if first:
                url_id = row[0]
                first = False
            else:
                raise Exception("%d has multiple redirects" % raw_url_id)

        return url_id

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
