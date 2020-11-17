#!/usr/bin/python3

# requires database filled by running condensate.py

import sys
from common import config, make_connection
from lang_wrap import init_lang_recog
from opt_util import get_quoted_list_option
from stem_mixin import StemMixin
from stop_util import load_stop_words
from token_util import tokenize
from word_freq_base import WordFreqBase, WordFreqPayload

class Payload(WordFreqPayload):
    def __init__(self, stop_set, top_word_count):
        WordFreqPayload.__init__(self, top_word_count)
        self.stop_set = stop_set

    def add(self, text):
        lst = tokenize(text, False)
        for w in lst:
            if w not in self.stop_set:
                cnt = self.word2freq.get(w, 0)
                self.word2freq[w] = cnt + 1


class Processor(WordFreqBase, StemMixin):
    def __init__(self, cur, stop_words, deconstructed):
        WordFreqBase.__init__(self, cur, deconstructed)
        StemMixin.__init__(self)
        self.stop_set = set(stop_words)
        self.lang_recog = init_lang_recog()

    def load_item(self, et):
        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            if self.load_text_party(et):
                self.extend_date(et)

    def load_text_party(self, et):
        variant = self.get_variant(et['osobaid'])
        if variant is None:
            return False

        txt = self.reconstitute(et)
        payload = self.variant2payload.get(variant)
        if not payload:
            payload = Payload(self.stop_set, self.top_word_count)
            self.variant2payload[variant] = payload

        payload.add(txt)
        return True


def main():
    if (len(sys.argv) == 2) and (sys.argv[1] == '--content-words-only'):
        config['root']['content_words_only'] = "1"

    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, stop_words, parties)
            try:
                processor.run()
                processor.dump_vocab()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
