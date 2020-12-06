#!/usr/bin/python3

# requires database filled by running condensate.py

import emoji
import regex
from common import make_connection
from opt_util import get_quoted_list_option
from word_freq_base import WordFreqBase, WordFreqPayload

class Processor(WordFreqBase):
    def __init__(self, cur, deconstructed):
        WordFreqBase.__init__(self, cur, deconstructed)

    def load_item(self, et):
        if self.load_text_party(et):
            self.extend_date(et)

    def find_emojis(self, et):
        lst = []
        txt = et['text']

        # https://stackoverflow.com/questions/43146528/how-to-extract-all-the-emojis-from-text/43146653
        data = regex.findall(r'\X', txt)
        for word in data:
            if any(char in emoji.UNICODE_EMOJI for char in word):
                lst.append(word)

        return lst

    def load_text_party(self, et):
        variant = self.get_variant(et['osobaid'])
        if variant is None:
            return False

        lst = self.find_emojis(et)
        payload = self.variant2payload.get(variant)
        if not payload:
            payload = WordFreqPayload(self.top_word_count)
            self.variant2payload[variant] = payload

        payload.add_list(lst)
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
