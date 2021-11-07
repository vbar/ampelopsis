import os
import re
import sys
from ufal.morphodita import *
from .pool import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from common import get_option
from opt_util import get_cache_path

pos_rx = re.compile('^[ACDINV]$')

segment_rx = re.compile('[-_:;^]')


def make_tagger():
    stemmer_data = get_cache_path(get_option("morphodita_tagger_file", "czech-morfflex-pdt-161115.tagger"))
    if not os.path.isfile(stemmer_data):
        raise Exception("required file %s not found" % stemmer_data)

    tagger = Tagger.load(stemmer_data)
    if not tagger:
        raise Exception("cannot create tagger from %s" % stemmer_data)

    return tagger


the_pool = Pool(make_tagger)


def stem_text(txt):
    if not txt:
        return None

    with the_pool.get_resource() as tagger:
        forms = Forms()
        lemmas = TaggedLemmas()
        tokens = TokenRanges()
        tokenizer = tagger.newTokenizer()
        if tokenizer is None:
            raise Exception("No tokenizer is defined for the supplied model!")

        tokenizer.setText(txt)
        rect = ""
        while tokenizer.nextSentence(forms, tokens):
            tagger.tag(forms, lemmas)

            lst = []
            for i in range(len(lemmas)):
                lemma_obj = lemmas[i]
                token_obj = tokens[i]
                raw_lemma = lemma_obj.lemma
                tag = lemma_obj.tag
                if tag and pos_rx.match(tag[0]):
                    sgm = segment_rx.split(raw_lemma)
                    if sgm:
                        w = sgm[0]
                        if len(w) > 1:
                            lst.append(w)

            if len(lst):
                ln = " ".join(lst)
                ln += "\n" # ".\n" we don't encourage searching for multiple sentences...
                rect += ln

    return rect
