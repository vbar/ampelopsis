#!/usr/bin/python3

# requires downloaded data extended by running majka-stemmer.py

import os
import re
import sys
from common import get_loose_path, make_connection
from cursor_wrapper import CursorWrapper
from token_util import url_rx

pos_filter_spec = {
    'noun': 1,
    'adjective': 2,
    'pronoun': 3,
    'numeral': 4,
    'verb': 5,
    'adverb': 6,
    'preposition': 7,
    'conjunction': 8,
    'particle': 9
    # 2-digit interjection is special
}

class StemException(Exception):
    pass


class MajkaTap(CursorWrapper):
    def __init__(self, cur, pos_filter=None):
        CursorWrapper.__init__(self, cur)
        self.fragment_rx = re.compile("^[^#]+#([0-9]+)$")
        if pos_filter:
            singles = []
            for pos in pos_filter:
                d = pos_filter_spec.get(pos)
                if d:
                    singles.append(d)

            singles.sort()
            pos_class_contents = "".join((str(s) for s in singles))
            pos_class = "[" + pos_class_contents + "]"

            if 'noun' in pos_filter:
                if 'interjection' in pos_filter:
                    pattern_body = pos_class
                else:
                    pattern_body = pos_class + "(?!0)"
            else:
                if 'interjection' in pos_filter:
                    pattern_body = "(?:" + pos_class + "|10)"
                else:
                    pattern_body = pos_class

            self.content_pos_rx = re.compile("^k" + pattern_body)
        else:
            self.content_pos_rx = None

    def reconstitute(self, url):
        mask = url + '#%'
        self.cur.execute("""select url, id
from field
where url like %s
order by url""", (mask,))
        rows = self.cur.fetchall()
        frag2ln = {}
        for surl, url_id in rows:
            frag = self.get_fragment(surl)
            if frag is not None:
                frag2ln[frag] = self.reconstitute_line(url_id)

        rect = []
        for frag, ln in sorted(frag2ln.items()):
            rect.append(ln)

        return "\n".join(rect)

    def reconstitute_line(self, url_id):
        sentence = []
        loose_path = get_loose_path(url_id, alt_repre='majka')
        try:
            with open(loose_path, "r", encoding ='utf-8') as f:
                for raw in f:
                    ln = raw.strip()
                    if url_rx.match(ln):
                        sentence.append(ln)
                    elif ln:
                        wln = ln.split(':', 1)
                        l = len(wln)
                        if not l or not wln[0]:
                            raise Exception(loose_path + ": no source word in " + ln)

                        if (l < 2) or not wln[1]:
                            sentence.append(ln)
                        else:
                            stem = self.get_stem(wln[1])
                            if stem:
                                sentence.append(stem)

            return " ".join(sentence)
        except StemException as ex:
            raise Exception(loose_path + ": " + str(ex))

    def get_fragment(self, surl):
        m = self.fragment_rx.match(surl)
        if not m:
            # alternative stemmers use fragments that fail to match
            # here
            return None

        return int(m.group(1))

    def get_stem(self, tail):
        items = tail.split(':')
        l = len(items)
        if (not l) or (l % 2):
            raise StemException("odd items in " + tail)

        stem2freq = {}
        for i in range(0, l, 2):
            if self.content_pos_rx:
                attrs = items[i + 1]
                accepted = self.content_pos_rx.match(attrs)
            else:
                accepted = True

            if accepted:
                stem = items[i]
                freq = stem2freq.get(stem, 0)
                stem2freq[stem] = freq + 1

        for stem, freq in sorted(stem2freq.items(), key=lambda p: (-1 * p[1], len(p[0]))):
            return stem

        return None


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            tap = MajkaTap(cur)
            for url in sys.argv[1:]:
                rect = tap.reconstitute(url)
                print(rect, "\n")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
