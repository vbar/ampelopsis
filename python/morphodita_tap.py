#!/usr/bin/python3

# requires downloaded data extended by running morphodita-stemmer.py

import collections
from io import BytesIO
from lxml import etree
import os
import re
import shutil
import sys
from common import get_loose_path, make_connection
from cursor_wrapper import CursorWrapper
from token_util import link_split_rx

SentenceModel = collections.namedtuple('SentenceModel', 'text stems tags')

stem_sep_rx = re.compile('[-_`]')

pos_filter_spec = {
    'noun': 'N',
    'adjective': 'A',
    'pronoun': 'P',
    'numeral': 'C',
    'verb': 'V',
    'adverb': 'D',
    'preposition': 'R',
    'conjunction': 'J',
    'particle': 'T',
    'interjection': 'I'
}

class MorphoditaTap(CursorWrapper):
    def __init__(self, cur, pos_filter=None):
        CursorWrapper.__init__(self, cur)
        if not pos_filter:
            self.tag_filter_rx = None
        else:
            singles = []
            for pos in pos_filter:
                singles.append(pos_filter_spec[pos])

            singles.sort()
            pos_class_contents = "".join((s for s in singles))
            pos_class = "[" + pos_class_contents + "]"
            self.tag_filter_rx = re.compile(pos_class) # using match, not search

    def reconstitute(self, url):
        url_id = self.get_url_id(url)
        if not url_id:
            print("url %s not found" % (url,), file=sys.stderr)
            return ""

        links = self.get_links(url_id)

        surl = url + '#plain'
        surl_id = self.get_url_id(surl)
        if not surl_id:
            print("no %s found" % (surl,), file=sys.stderr)
            return ""

        root = self.get_xml_document(surl_id)
        if not root:
            print("no %s" % (surl,), file=sys.stderr)
            return ""

        sentences = root.xpath("/doc/sentence")
        rect = []
        for s in sentences:
            ln = self.reconstitute_line(links, s)
            if ln:
                rect.append(ln)

        return "\n".join(rect)

    def remodel(self, url):
        surl = url + '#plain'
        surl_id = self.get_url_id(surl)
        if not surl_id:
            print("remodel: no %s found" % (surl,), file=sys.stderr)
            return []

        root = self.get_xml_document(surl_id)
        if not root:
            print("remodel: no %s" % (surl,), file=sys.stderr)
            return []

        sentence_models = []
        sentences = root.xpath("/doc/sentence")
        for s in sentences:
            sentence_models.append(self.remodel_sentence(s))

        return sentence_models

    def get_url_id(self, url):
        self.cur.execute("""select id
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            return None

        return row[0]

    def get_links(self, url_id):
        src_path = get_loose_path(url_id, alt_repre='morphodita')
        if not os.path.exists(src_path):
            # print("no morphodita input", file=sys.stderr)
            return []

        with open(src_path, 'r') as infile:
            txt = infile.read()
            seq = txt.split()
            lst = []
            for w in seq:
                if w[0] in ('@', '#'):
                    sseq = link_split_rx.split(w)
                    lst.append(sseq[0])

            return lst

    def get_xml_document(self, url_id):
        src_path = get_loose_path(url_id, alt_repre='morphodita')
        if os.path.exists(src_path):
            with open(src_path, 'rb') as infile:
                with BytesIO() as whole:
                    whole.write(b"<doc>")
                    shutil.copyfileobj(infile, whole)
                    whole.write(b"</doc>")
                    whole.seek(0)
                    return etree.parse(whole)

        return None

    def is_valid(self, tag):
        return (tag != 'Z:-------------') and ((not self.tag_filter_rx) or self.tag_filter_rx.match(tag))

    def reconstitute_line(self, links, sentence):
        words = []
        tokens = sentence.xpath("./token")
        for token in tokens:
            tag = token.get('tag')
            if tag:
                lemma = token.get('lemma')
                if lemma in ('@', '#'):
                    if len(links) and (links[0][0] == lemma):
                        words.append(links.pop(0))
                    else:
                        words.append(lemma) # should anything be done with links here?
                elif self.is_valid(tag):
                    segments = stem_sep_rx.split(lemma, 2)
                    if len(segments) and segments[0]:
                        words.append(segments[0])

        return " ".join(words)

    def remodel_sentence(self, sentence):
        t = ""
        for w in sentence.itertext():
            t += w

        tags = []
        words = []
        tokens = sentence.xpath("./token")
        for token in tokens:
            tag = token.get('tag')
            if self.is_valid(tag):
                lemma = token.get('lemma')
                if lemma:
                    segments = stem_sep_rx.split(lemma, 2)
                    if len(segments) and segments[0]:
                        tags.append(tag)
                        words.append(segments[0])

        return SentenceModel(text=t, stems=words, tags=tags)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            tap = MorphoditaTap(cur)
            for url in sys.argv[1:]:
                rect = tap.reconstitute(url)
                print(rect, "\n")


if __name__ == "__main__":
    main()
