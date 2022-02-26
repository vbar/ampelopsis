#!/usr/bin/python3

import os
import re
import sys
from ufal.morphodita import *
from common import get_option
from opt_util import get_cache_path

pos_rx = re.compile('^[ACDINV]$')

segment_rx = re.compile('[-_:;^]')

tail_rx = re.compile('.([-_:;^].+)$')

name_tag_rx = re.compile("^NN[MF]S1-----A----$")

def make_tagger():
    stemmer_data = get_cache_path(get_option("morphodita_tagger_file", "czech-morfflex-pdt-161115.tagger"))
    if not os.path.isfile(stemmer_data):
        raise Exception("required file %s not found" % stemmer_data)

    tagger = Tagger.load(stemmer_data)
    if not tagger:
        raise Exception("cannot create tagger from %s" % stemmer_data)

    return tagger


def tokenize_fulltext(tagger, txt):
    forms = Forms()
    lemmas = TaggedLemmas()
    tokens = TokenRanges()
    tokenizer = tagger.newTokenizer()
    if tokenizer is None:
        raise Exception("No tokenizer is defined for the supplied model!")

    tokenizer.setText(txt)
    matrix = []
    while tokenizer.nextSentence(forms, tokens):
        tagger.tag(forms, lemmas)

        lst = []
        for lemma_obj in lemmas:
            raw_lemma = lemma_obj.lemma
            tag = lemma_obj.tag
            if tag and pos_rx.match(tag[0]):
                sgm = segment_rx.split(raw_lemma)
                if sgm:
                    w = sgm[0]
                    if len(w) > 1:
                        lst.append(w)

        if len(lst):
            matrix.append(lst)

    return matrix


def retrieve_annotations(tagger, txt):
    forms = Forms()
    lemmas = TaggedLemmas()
    tokens = TokenRanges()
    tokenizer = tagger.newTokenizer()
    if tokenizer is None:
        raise Exception("No tokenizer is defined for the supplied model!")

    tokenizer.setText(txt)
    matrix = []
    while tokenizer.nextSentence(forms, tokens):
        tagger.tag(forms, lemmas)

        lst = []
        for lemma_obj in lemmas:
            raw_lemma = lemma_obj.lemma
            tag = lemma_obj.tag
            if tag:
                m = tail_rx.search(raw_lemma)
                if m:
                    head = raw_lemma[0:m.start(1)]
                    tail = m.group(1)
                    lst.append((head, tail))

        if len(lst):
            matrix.append(lst)

    return matrix


def filter_matrix(raw_matrix, stop_set):
    matrix = []
    for raw_sentence in raw_matrix:
        sentence = []
        for w in raw_sentence:
            if w.lower() not in stop_set:
                sentence.append(w)

        if len(sentence):
            matrix.append(sentence)

    return matrix


def collate_matrix(matrix):
    rect = ""
    for sentence in matrix:
        ln = " ".join(sentence)
        ln += ".\n"
        rect += ln

    return rect


def simplify_fulltext(tagger, stop_set, txt):
    full_matrix = tokenize_fulltext(tagger, txt)
    matrix = filter_matrix(full_matrix, stop_set)
    return collate_matrix(matrix)


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
            while i > 0:
                lemma_obj = lemmas[i - 1]
                tag = lemma_obj.tag
                matching = False
                if name_tag_rx.match(tag):
                    raw_lemma = lemma_obj.lemma
                    semi_pos = raw_lemma.find("_;")
                    if semi_pos > 0:
                        lemma = raw_lemma[:semi_pos]
                        lemma_tail = raw_lemma[semi_pos+2:semi_pos+3]
                        # some first names (e.g. Filip) are also last
                        # names; some last names (e.g. Rakušan) are
                        # classified as nationalities
                        if lemma_tail in ('S', 'Y', 'E'):
                            matching = True
                            if tailing:
                                token = tokens[i - 1]
                                if token.length:
                                    stretch = txt[token.start : token.start + token.length].strip()
                                    if stretch:
                                        rev_tail.append(stretch)
                            else:
                                # text has multiple names; this is
                                # probably incorrect for e.g. Ursula
                                # von der Leyen, but we can add a test
                                # for lemmas in the middle of a name
                                # here when we see them
                                return None
                i -= 1
                if not matching:
                    tailing = False
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


def main():
    print("loading tagger...", file=sys.stderr)
    tagger = make_tagger()
    for a in sys.argv[1:]:
        pn = split_position_name(tagger, a)
        if pn:
            print(pn[1], pn[0])
        else:
            print(a)


if __name__ == "__main__":
    main()
