#!/usr/bin/python3

# requires database filled by running condensate.py

import json
from common import get_option, make_connection
from lang_wrap import init_lang_recog
from opt_util import get_quoted_list_option
from pinhole_base import PinholeBase
from stem_recon import reconstitute
from stop_util import load_stop_words
from token_util import tokenize
from url_heads import town_url_head

class Payload:
    def __init__(self, stop_set, top_word_count):
        self.stop_set = stop_set
        self.top_word_count = top_word_count
        self.word2freq = {}

    def get_total(self):
        return sum(self.word2freq.values())

    def get_tops(self):
        words = []
        for word, freq in sorted(self.word2freq.items(), key=lambda p: (-1 * p[1], p[0])):
            if freq == 1:
                return words

            words.append(word)
            if len(words) == self.top_word_count:
                return words

        return words

    def get_freq(self, w):
        return self.word2freq[w]

    def add(self, text):
        lst = tokenize(text, False)
        for w in lst:
            if w not in self.stop_set:
                cnt = self.word2freq.get(w, 0)
                self.word2freq[w] = cnt + 1


class Processor(PinholeBase):
    def __init__(self, cur, stop_words, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        self.stop_set = set(stop_words)
        self.top_word_count = int(get_option("top_word_count", "3"))
        self.lang_recog = init_lang_recog()
        self.variant2payload = {}
        self.reconstitute = self.reconstitute_rect if get_option("use_stemmed", True) else self.reconstitute_simple

    def dump(self):
        name2desc = {}
        data = []
        for variant, payload in sorted(self.variant2payload.items(), key=lambda p: (-1 * p[1].get_total(), str(p[0]))):
            ext_url = None
            if type(variant) is str:
                name = self.person_map[variant]
                town_name = self.hamlet2town.get(variant)
                if town_name:
                    ext_url = "%s/%s" % (town_url_head, town_name)
            else:
                name = self.party_map[variant]

            color = self.introduce_color(variant)

            item = {
                'color': color,
                'total': payload.get_total()
            }

            if ext_url is not None:
                item['ext_url'] = ext_url

            name2desc[name] = item

            tops = payload.get_tops()
            for w in tops:
                item = [w, name, payload.get_freq(w)]
                data.append(item)

        custom = {
            'parties': name2desc,
            'data': sorted(data, key=lambda t: -1 * t[2]),
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2))

    def load_item(self, et):
        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            if self.load_text_party(et):
                self.extend_date(et)

    def reconstitute_rect(self, et):
        return reconstitute(self.cur, et['url'])

    def reconstitute_simple(self, et):
        lst = tokenize(et['text'], True)
        return " ".join(lst)

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
    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            parties = get_quoted_list_option("selected_parties", [])
            processor = Processor(cur, stop_words, parties)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
