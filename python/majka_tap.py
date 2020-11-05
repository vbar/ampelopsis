#!/usr/bin/python3

# requires downloaded data extended by running majka-stemmer.py

import os
import re
import sys
from common import get_loose_path, make_connection
from cursor_wrapper import CursorWrapper
from token_util import url_rx

class StemException(Exception):
    pass


class MajkaTap(CursorWrapper):
    def __init__(self, cur, content_words_only=False):
        CursorWrapper.__init__(self, cur)
        self.fragment_rx = re.compile("^[^#]+#([0-9]+)$")
        if content_words_only:
            self.content_pos_rx = re.compile("^k[15]")
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
    with make_connection() as conn:
        with conn.cursor() as cur:
            tap = MajkaTap(cur)
            for url in sys.argv[1:]:
                rect = tap.reconstitute(url)
                print(rect, "\n")


if __name__ == "__main__":
    main()
