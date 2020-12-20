import json
from common import get_option
from pinhole_base import PinholeBase
from url_heads import town_url_head

class WordFreqPayload:
    def __init__(self, top_word_count):
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

    def add_list(self, lst):
        for w in lst:
            cnt = self.word2freq.get(w, 0)
            self.word2freq[w] = cnt + 1


class WordFreqBase(PinholeBase):
    def __init__(self, cur, deconstructed):
        PinholeBase.__init__(self, cur, False, deconstructed)
        self.variant2payload = {} # str variant -> instance of class derived from WordFreqPayload
        self.top_word_count = int(get_option("top_word_count", "3"))

    def enrich_freq(self, gf):
        pass

    def dump_vocab(self):
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

        self.enrich_freq(custom)

        print(json.dumps(custom, indent=2))
