from common import get_option
from majka_tap import MajkaTap
from token_util import tokenize

class StemMixin: # self.cur must be provided by another inherited class
    def __init__(self, content_words_only=False):
        if get_option("use_stemmed", True):
            self.tap = MajkaTap(self.cur, content_words_only)
            self.reconstitute = self.reconstitute_rect
        else:
            self.reconstitute = self.reconstitute_simple

    def reconstitute_rect(self, et):
        return self.tap.reconstitute(et['url'])

    def reconstitute_simple(self, et):
        lst = tokenize(et['text'], True)
        return " ".join(lst)
