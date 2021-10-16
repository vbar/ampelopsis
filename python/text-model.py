#!/usr/bin/python3

import json
import sys
from common import get_loose_path, get_option, make_connection
from show_room import ShowRoom

class Processor(ShowRoom):
    def __init__(self, cur):
        ShowRoom.__init__(self, cur)
        self.storage_alternative = get_option("storage_alternative", None)

    def load_item(self, doc):
        assert self.storage_alternative

        url = doc.get('url')
        url_id = self.get_url_id(url)
        if url_id:
            name = doc.get('nazevMaterialu', "")
            desc = doc.get('popis', "")
            if name or desc:
                txt = "%s\n\n%s" % (name, desc)
                self.write_alt(url_id, txt)

        attachments = doc.get('prilohy')
        if isinstance(attachments, list):
            for att in attachments:
                url = att['DocumentUrl']
                url_id = self.get_url_id(url)
                if url_id:
                    txt = att.get('DocumentPlainText')
                    if txt:
                        self.write_alt(url_id, txt)

    def write_alt(self, url_id, txt):
        loose_path = get_loose_path(url_id, alt_repre=self.storage_alternative)
        with open(loose_path, 'w') as f:
            f.write(txt)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            if not processor.storage_alternative:
                print("no storage alternative set", file=sys.stderr)
                return

            try:
                processor.run()
            finally:
                processor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
