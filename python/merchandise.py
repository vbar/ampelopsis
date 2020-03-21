import sys
from show_case import ShowCase

class MerchandiSelector(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.hamlet2count = {} # str -> int

    def load_page(self, page_url, url_id):
        doc = self.get_document(url_id)
        if not doc:
            print(page_url + " not found on disk", file=sys.stderr)
            return

        print("loading %s..." % (page_url,), file=sys.stderr)
        items = doc.get('results')
        for et in items:
            hamlet_name = et.get('osobaid')
            count = self.hamlet2count.get(hamlet_name, 0)
            self.hamlet2count[hamlet_name] = count + 1

    def get_top_contributors(self, count):
        # FIXME: use heap
        contributors = [ contr for contr, cnt in sorted(self.hamlet2count.items(), key=lambda kv: -1 * kv[1]) ]
        if len(contributors) < count:
            return contributors
        else:
            return contributors[:count]
