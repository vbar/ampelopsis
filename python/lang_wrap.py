from langid.langid import LanguageIdentifier, model
from common import get_option

class LangidWrapper:
    def __init__(self):
        self.identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
        self.identifier.set_languages(['cs', 'de', 'en', 'ru'])
        self.threshold = float(get_option("langid_prob_threshold", "0.5"))

    def check(self, word_list):
        p = self.identifier.classify(" ".join(word_list))
        if p and (p[1] > self.threshold):
            return p[0]
        else:
            return None

def init_lang_recog():
    return LangidWrapper()
