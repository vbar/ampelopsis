#!/usr/bin/python3

# requires database filled by running condensate.py

import re
from common import make_connection
from opt_util import get_quoted_list_option
from word_freq_base import WordFreqBase, WordFreqPayload

class Payload(WordFreqPayload):
    def __init__(self, top_word_count):
        WordFreqPayload.__init__(self, top_word_count)

    def add(self, lst):
        for w in lst:
            cnt = self.word2freq.get(w, 0)
            self.word2freq[w] = cnt + 1


class Processor(WordFreqBase):
    def __init__(self, cur, deconstructed):
        WordFreqBase.__init__(self, cur, deconstructed)
        self.tag_rx = re.compile('(#[-\\w]+)')

    def load_item(self, et):
        if self.load_text_party(et):
            self.extend_date(et)

    def find_tags(self, et):
        lst = []
        txt = et['text']
        for m in self.tag_rx.finditer(txt):
            lst.append(m.group(1))

        return lst

    def load_text_party(self, et):
        variant = self.get_variant(et['osobaid'])
        if variant is None:
            return False

        lst = self.find_tags(et)
        payload = self.variant2payload.get(variant)
        if not payload:
            payload = Payload(self.top_word_count)
            self.variant2payload[variant] = payload

        payload.add(lst)
        return True


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, parties)
            try:
                processor.run()
                processor.dump_vocab()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
