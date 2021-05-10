import json
import os
from common import get_loose_path
from cursor_wrapper import CursorWrapper
from volume_holder import VolumeHolder

class LeafLoader(VolumeHolder, CursorWrapper):
    def __init__(self, cur):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)

    def load_statement(self, person_id, statement_id):
        url = self.make_statement_url(person_id, statement_id)
        url_id = self.get_url_id(url)
        if not url_id:
            return None

        return self.get_old_doc(url_id)

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            return None

        return row[0]

    def make_statement_url(self, person_id, statement_id):
        return "https://cro.justice.cz/verejnost/api/funkcionari/%s/oznameni/%s" % (person_id, statement_id)

    def make_gate_url(self, person_id):
        return "https://cro.justice.cz/verejnost/api/funkcionari/schvaleno/" + person_id

    def get_old_doc(self, url_id):
        buf = self.get_old_body(url_id)
        if not buf: # empty bodies considered unsatisfactory
            return None

        return self.safe_parse(buf)

    def get_old_body(self, url_id):
        # archiving is supported for post-processing but not during
        # download, which overwrites existing URLs
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

        return buf

    def safe_parse(self, body):
        try:
            return json.loads(body.decode('utf-8'))
        except Exception as ex:
            return None
