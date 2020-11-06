#!/usr/bin/python3

# requires downloaded data extended by running morphodita-stemmer.py

from lxml import etree
from io import BytesIO
import re
import shutil
import sys
from common import get_loose_path, make_connection
from cursor_wrapper import CursorWrapper

class MorphoditaTap(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)

    def reconstitute(self, url):
        surl = url + '#plain'
        surl_id = self.get_url_id(surl)
        if not surl_id:
            print("no %s" % (surl,), file=sys.stderr)
            return ""

        root = self.get_xml_document(surl_id)
        sentences = root.xpath("/doc/sentence")
        rect = []
        for s in sentences:
            ln = self.reconstitute_line(s)
            if ln:
                rect.append(ln)

        return "\n".join(rect)

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            return None

        return row[0]

    def get_xml_document(self, url_id):
        src_path = get_loose_path(url_id, alt_repre='morphodita')
        with open(src_path, 'rb') as infile:
            with BytesIO() as whole:
                whole.write(b"<doc>\n")
                shutil.copyfileobj(infile, whole)
                whole.write(b"</doc>\n")
                whole.seek(0)
                return etree.parse(whole)

    def reconstitute_line(self, sentence):
        words = []
        tokens = sentence.xpath("./token")
        for token in tokens:
            tag = token.get('tag')
            if tag and tag != 'Z:-------------':
                lemma = token.get('lemma')
                segments = re.split('[-_]', lemma, 2)
                if len(segments) and segments[0]:
                    words.append(segments[0])

        return " ".join(words)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            tap = MorphoditaTap(cur)
            for url in sys.argv[1:]:
                rect = tap.reconstitute(url)
                print(rect, "\n")


if __name__ == "__main__":
    main()
