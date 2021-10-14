from dateutil.parser import parse
import sys
from json_frame import JsonFrame

class ShowCabinet(JsonFrame):
    def __init__(self, cur, silent=False):
        JsonFrame.__init__(self, cur)
        self.silent = silent
        self.mindate = None
        self.maxdate = None

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        if not self.silent:
            print("loading %s..." % (page_url,), file=sys.stderr)

        attachments = doc.get('prilohy')
        if isinstance(attachments, list):
            for att in attachments:
                self.load_item(att)

    def extend_date(self, att):
        dt = parse(att['datumVlozeniPrilohy'])
        if (self.mindate is None) or (dt < self.mindate):
            self.mindate = dt

        if (self.maxdate is None) or (dt > self.maxdate):
            self.maxdate = dt

        return dt

    def make_date_extent(self):
        return [dt.isoformat() for dt in (self.mindate, self.maxdate)]
