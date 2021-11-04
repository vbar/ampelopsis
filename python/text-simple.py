#!/usr/bin/python3

# requires downloaded data extended by running morphodita-stemmer.py

from io import BytesIO
from lxml import etree
import os
import re
import sys

from common import get_loose_path, get_option, make_connection
from show_case import ShowCase

pos_rx = re.compile('^[ACDINV]$')

segment_rx = re.compile('[-_:;^]')

invalid_bytes_rx = re.compile(b'[^\x09\x0A\x0D\x20-\xff]')

invalid_str_rx = re.compile('\uFFFE|\uFFFF')

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.morpho_repre = get_option("morphodita_output", "morpho")
        self.simple_repre = get_option("simple_representation", "simple")
        self.simple_size_limit = int(get_option("simple_size_limit", "500000"))
        self.buffer_size = 1024

    def load_item(self, rec):
        ext_url = rec['url']
        order = rec.get('poradi')
        if order:
            url = "%s#%s" % (ext_url, order)
        else:
            url = ext_url

        self.simplify(url, rec['url_id'])

    def simplify(self, url, url_id):
        root = self.get_xml_document(url_id)
        if not root:
            return

        print("simplifying %s..." % (url,), file=sys.stderr)
        dst_path = get_loose_path(url_id, alt_repre=self.simple_repre)
        outfile = None
        try:
            sentences = root.xpath("/doc/sentence")
            sz = 0
            for s in sentences:
                ln = self.make_line(s)
                if ln is not None:
                    sz += len(ln)
                    if sz > self.simple_size_limit:
                        return

                    if outfile is None:
                        outfile = open(dst_path, 'w', encoding='utf-8')

                    outfile.write(ln)

            if (outfile is None) and os.path.exists(dst_path):
                os.unlink(dst_path)
        finally:
            if outfile is not None:
                outfile.close()

    def get_xml_document(self, url_id):
        src_path = get_loose_path(url_id, alt_repre=self.morpho_repre)
        if os.path.exists(src_path):
            with open(src_path, 'rb') as infile:
                with BytesIO() as whole:
                    whole.write(b"<doc>")
                    self.copy_morpho(infile, whole)
                    whole.write(b"</doc>")
                    whole.seek(0)
                    return etree.parse(whole)

        return None

    def copy_morpho(self, infile, whole):
        while True:
            buf = infile.read(self.buffer_size)
            while (len(buf) >= self.buffer_size) and (buf[-1] & 0x80):
                b = infile.read(1)
                if len(b):
                    buf += b
                else:
                    break

            if len(buf):
                raw_bytes = invalid_bytes_rx.sub(b'', buf)
                raw_str = raw_bytes.decode('utf-8', 'ignore')
                clean_str = invalid_str_rx.sub('', raw_str)
                clean_bytes = clean_str.encode('utf-8')
                whole.write(clean_bytes)
            else:
                return

    def make_line(self, sentence):
        lst = []
        for token in sentence.xpath("./token"):
            raw_lemma = token.get('lemma')
            tag = token.get('tag')
            if tag and pos_rx.match(tag[0]):
                sgm = segment_rx.split(raw_lemma)
                if sgm:
                    w = sgm[0]
                    if len(w) > 1:
                        lst.append(w)

        if len(lst):
            ln = " ".join(lst)
            ln += ".\n"
            return ln
        else:
            return None


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
