from common import get_option
from majka_tap import MajkaTap
from morphodita_tap import MorphoditaTap
from token_util import tokenize

class StemMixin: # self.cur must be provided by another inherited class
    def __init__(self, content_words_only=False):
        stemmer = get_option("active_stemmer", "morphodita")
        if stemmer:
            if stemmer == "majka":
                self.tap = MajkaTap(self.cur, content_words_only)
            elif stemmer == "morphodita":
                self.tap = MorphoditaTap(self.cur, content_words_only)
            else:
                raise Exception("unknown stemmer: " + stemmer)

            self.reconstitute = self.reconstitute_rect
        else:
            self.reconstitute = self.reconstitute_simple

    def reconstitute_rect(self, et):
        return self.tap.reconstitute(et['url'])

    def reconstitute_simple(self, et):
        lst = tokenize(et['text'], True)
        return " ".join(lst)
