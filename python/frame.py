import sys
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class Frame(VolumeHolder, CursorWrapper):
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
