import json
import re
import sys
from urllib.parse import quote

class JsonParser:
    schema = re.compile("^https://cro.justice.cz/verejnost/api/funkcionari\\?order=DESC&page=(\\d+)&pageSize=(\\d+)&sort=created$")

    def __init__(self, owner, url):
        m = self.schema.match(url)
        if m:
            self.owner = owner
            self.page = int(m.group(1))
            self.page_size = int(m.group(2))
        else:
            print("skipping %s with unknown schema" % url, file=sys.stderr)
            self.owner = None

    def parse_links(self, fp):
        if not self.owner:
            return

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))

        if self.page == 0:
            count = int(doc.get('count'))
            n = count // self.page_size
            i = 0
            while i <= n:
                url = "https://cro.justice.cz/verejnost/api/funkcionari?order=DESC&page=%d&pageSize=%d&sort=created" % (i, self.page_size)
                self.owner.add_link(url)
                i += 1

        items = doc.get('items')
        for person in items:
            person_id = person.get('id')
            url = "https://cro.justice.cz/verejnost/api/funkcionari/%s" % person_id
            self.owner.add_link(url)

            # enrich from Wikidata (not finished)
            name = "%s %s" % (person.get('firstName'), person.get('lastName'))
            url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&language=cs&search=%s" % quote(name)
            self.owner.add_link(url)
