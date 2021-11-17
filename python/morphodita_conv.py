import os
import re
from ufal.morphodita import *
from common import get_option
from opt_util import get_cache_path

lemma_tail_rx = re.compile("^([^-]+)-[0-9]+$")

def make_tagger():
    stemmer_data = get_cache_path(get_option("morphodita_tagger_file", "czech-morfflex-pdt-161115.tagger"))
    if not os.path.isfile(stemmer_data):
        raise Exception("required file %s not found" % stemmer_data)

    tagger = Tagger.load(stemmer_data)
    if not tagger:
        raise Exception("cannot create tagger from %s" % stemmer_data)

    return tagger


def split_position_name(tagger, txt, strictly_sentence=False):
    forms = Forms()
    lemmas = TaggedLemmas()
    tokens = TokenRanges()
    tokenizer = tagger.newTokenizer()
    if tokenizer is None:
        raise Exception("No tokenizer is defined for the supplied model!")

    tokenizer.setText(txt)
    sentence_no = 0
    rev_tail = []
    while tokenizer.nextSentence(forms, tokens):
        if sentence_no == 0:
            tagger.tag(forms, lemmas)

            i = len(lemmas)
            tailing = True
            while (i > 0) and tailing:
                lemma_obj = lemmas[i - 1]
                tag = lemma_obj.tag
                old_length = len(rev_tail)
                if tag == 'NNMS1-----A----':
                    raw_lemma = lemma_obj.lemma
                    semi_pos = raw_lemma.find("_;")
                    if semi_pos > 0:
                        lemma = raw_lemma[:semi_pos]
                        lemma_tail = raw_lemma[semi_pos+2:]
                        # some first names (e.g. Filip) are also last names
                        if lemma_tail in ('S', 'Y'):
                            m = lemma_tail_rx.match(lemma)
                            lemma_head = m.group(1) if m else lemma
                            rev_tail.append(lemma_head)

                i -= 1
                tailing = len(rev_tail) > old_length
        else:
            # only accepting single-sentence paragraphs
            if strictly_sentence:
                raise Exception("unexpected multiple sentences")
            else:
                return None

        sentence_no += 1

    if not len(rev_tail):
        return None

    name = " ".join(reversed(rev_tail))
    if not txt.endswith(name):
        raise Exception("unclean text: " + txt)

    l = len(txt) - len(name)
    position = txt[:l]
    return (position.rstrip(), name)
