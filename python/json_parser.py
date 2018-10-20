import json
import re
import sys
from jump_util import make_search_url, make_query_url

class JsonParser:
    schema = re.compile("^https://cro.justice.cz/verejnost/api/funkcionari\\?order=DESC&page=(\\d+)&pageSize=(\\d+)&sort=created$")

    # should use search_url_head (with quoted '?')
    search_schema = re.compile("^https://www.wikidata.org/w/api.php\\?action=wbsearchentities&format=json&language=cs&search=")

    wid_rx = re.compile('^Q[0-9]+$')

    def __init__(self, owner, url):
        m = self.schema.match(url)
        if m:
            self.owner = owner
            self.page_size = int(m.group(2))
            self.page = int(m.group(1))
        else:
            m = self.search_schema.match(url)
            if m:
                self.owner = owner
                self.page_size = None
            else:
                print("skipping %s with unknown schema" % url, file=sys.stderr)
                self.owner = None
                self.page_size = None

    def parse_links(self, fp):
        if not self.owner:
            return

        buf = b''
        for ln in fp:
            buf += ln

        doc = json.loads(buf.decode('utf-8'))
        if self.page_size is not None:
            self.process_listing(doc)
        else:
            self.process_search(doc)

    def process_listing(self, doc):
        if self.page == 0:
            count = int(doc.get('count'))
            n = count // self.page_size
            i = 1
            while i <= n:
                url = "https://cro.justice.cz/verejnost/api/funkcionari?order=DESC&page=%d&pageSize=%d&sort=created" % (i, self.page_size)
                self.owner.add_link(url)
                i += 1

        items = doc.get('items')
        for person in items:
            person_id = person.get('id')
            url = "https://cro.justice.cz/verejnost/api/funkcionari/%s" % person_id
            self.owner.add_link(url)

            # enrich from Wikidata
            url = make_search_url(person.get('firstName'), person.get('lastName'))
            self.owner.add_link(url)

    def process_search(self, doc):
        lst = doc.get('search')
        for it in lst:
            wid = it.get('title')
            if not self.wid_rx.match(wid):
                print(it, file=sys.stderr)
                raise Exception("unexpected ID " + wid)

            url = make_query_url(wid)
            self.owner.add_link(url)
