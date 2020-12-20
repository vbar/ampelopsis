#!/usr/bin/python3

# requires database filled by running condensate.py

from common import make_connection
from emoji_util import get_emojis, get_emoji_hex
from opt_util import get_quoted_list_option
from word_freq_base import WordFreqBase, WordFreqPayload

class Processor(WordFreqBase):
    def __init__(self, cur, deconstructed):
        WordFreqBase.__init__(self, cur, deconstructed)

    def load_item(self, et):
        if self.load_text_party(et):
            self.extend_date(et)

    def enrich_freq(self, gf):
        emo2title = {}
        for row in gf['data']:
            emo = row[0]
            if emo not in emo2title:
                hexes = get_emoji_hex(emo)
                emo2title[emo] = " ".join(hexes)

        gf['titlemap'] = emo2title

    def load_text_party(self, et):
        variant = self.get_variant(et['osobaid'])
        if variant is None:
            return False

        lst = get_emojis(et['text'])
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
