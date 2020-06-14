#!/usr/bin/python3

# requires database filled by running condensate.py

import numpy as np
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import sys
from analyzer import Analyzer
from common import get_option, make_connection
from distance_args import ConfigArgs
from lang_wrap import init_lang_recog
from pinhole_base import PinholeBase
from stem_recon import reconstitute
from stop_util import load_stop_words
from token_util import tokenize

class Payload:
    def __init__(self, sep, text):
        self.sep = sep
        self.text = text
        self.count = 1

    def append(self, text):
        self.text = self.sep.join((self.text, text))
        self.count += 1


class Processor(PinholeBase):
    def __init__(self, cur, stop_words):
        PinholeBase.__init__(self, cur, False, '*')
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))
        self.stop_words = stop_words
        self.lang_recog = init_lang_recog()
        self.variant2payload = {}
        if get_option("use_stemmed", True):
            self.payload_separator = "\n"
            self.reconstitute = self.reconstitute_rect
        else:
            self.payload_separator = " "
            self.reconstitute = self.reconstitute_simple

    def load_item(self, et):
        if self.is_redirected(et['url']):
            return

        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            if self.load_text_person(et):
                self.extend_date(et)

    def load_text_person(self, et):
        variant = self.get_variant(et['osobaid'])
        if variant is None:
            return False

        txt = self.reconstitute(et)
        payload = self.variant2payload.get(variant)
        if not payload:
            self.variant2payload[variant] = Payload(self.payload_separator, txt)
        else:
            payload.append(txt)

        return True

    def reconstitute_rect(self, et):
        return reconstitute(self.cur, et['url'])

    def reconstitute_simple(self, et):
        lst = tokenize(et['text'], True)
        return " ".join(lst)

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        for gn in gd['nodes']:
            node_idx = gn['node']
            variant = self.node2variant[node_idx]
            payload = self.variant2payload[variant]
            gn['doc_count'] = payload.count

    # based on https://stackoverflow.com/questions/12118720/python-tf-idf-cosine-to-find-document-similarity
    def process(self):
        vzr = TfidfVectorizer(analyzer=Analyzer(self.stop_words))

        docs = []
        variants = self.get_doc_keys()
        for variant in variants:
            payload = self.variant2payload[variant]
            docs.append(payload.text)

        if not len(docs): # seen w/ incorrect download config
            raise Exception("No input documents found - check filtering conditions in load_item.")

        tfidf = vzr.fit_transform(docs)
        matrix = linear_kernel(tfidf, tfidf)
        shape = matrix.shape
        mn = np.min(matrix)
        mx = np.max(matrix)
        print("matrix %d x %d, from %f to %f" % (shape[0], shape[1], mn, mx), file=sys.stderr)

        l = len(docs)
        for i in range(l):
            for j in range(i + 1, l):
                if matrix[i][j] > self.link_threshold:
                    low_node = self.introduce_node(variants[i], False)
                    high_node = self.introduce_node(variants[j], False)
                    edge = (low_node, high_node)
                    self.ref_map[edge] = matrix[i][j]

    def get_doc_keys(self):
        head = []
        tail = []
        for variant in self.variant2payload.keys():
            if type(variant) is str:
                head.append(variant)
            else:
                tail.append(variant)

        head.sort()
        tail.sort()

        head.extend(tail)
        return head

def main():
    ca = ConfigArgs()
    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur, stop_words)
            try:
                processor.run()
                processor.process()
                processor.dump_undirected()
                if ca.histogram:
                    processor.dump_distance_histogram(ca.histogram)
            finally:
                processor.close()


if __name__ == "__main__":
    main()
