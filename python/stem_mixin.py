from common import get_option
from majka_tap import MajkaTap
from morphodita_tap import MorphoditaTap
from token_util import tokenize

parts_of_speech = frozenset(['noun', 'adjective', 'pronoun', 'numeral', 'verb', 'adverb', 'preposition', 'conjunction', 'particle', 'interjection'])

def expand_pos(abbrev):
    found = None
    for pos in parts_of_speech:
        if pos.startswith(abbrev):
            if found:
                raise Exception("ambiguous abbreviation " + abbrev)
            else:
                found = pos

    if not found:
        raise Exception("unknown abbreviation " + abbrev)

    return found


def parse_pos_option(raw):
    if not raw:
        return None

    txt = raw.lower()
    flt = set()
    for abbrev in txt.split():
        if abbrev:
            pos = expand_pos(abbrev)
            if pos in flt:
                raise Exception("%s repeated in \"%s\"" % (pos, raw))

            flt.add(pos)

    return flt


class StemMixin: # self.cur must be provided by another inherited class
    def __init__(self):
        stemmer = get_option("active_stemmer", "morphodita")
        if stemmer:
            stem_pos_filter = get_option("stem_pos_filter", None)
            tap_pos_filter = parse_pos_option(stem_pos_filter)

            if stemmer == "majka":
                self.tap = MajkaTap(self.cur, tap_pos_filter)
            elif stemmer == "morphodita":
                self.tap = MorphoditaTap(self.cur, tap_pos_filter)
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
