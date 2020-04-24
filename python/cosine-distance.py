#!/usr/bin/python3

import csv
import json
import networkx as nx
import numpy as np
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import sys
from common import get_option, make_connection
from lang_wrap import init_lang_recog
from pinhole_base import PinholeBase
from stop_util import load_stop_words
from token_util import tokenize, retokenize

class Payload:
    def __init__(self, text):
        self.text = text
        self.count = 1

    def append(self, text):
        self.text = " ".join((self.text, text))
        self.count += 1


class Processor(PinholeBase):
    def __init__(self, cur, stop_words):
        PinholeBase.__init__(self, cur, False, '*')
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))
        self.stop_words = stop_words
        self.lang_recog = init_lang_recog()
        self.variant2payload = {}

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

        lst = self.tokenize(et['text'])
        assert lst # caller checks language, which cannot succeed on empty text

        txt = " ".join(lst)
        payload = self.variant2payload.get(variant)
        if not payload:
            self.variant2payload[variant] = Payload(txt)
        else:
            payload.append(txt)

        return True

    @staticmethod
    def tokenize(txt):
        return tokenize(txt, True)

    def dump(self):
        ebunch = [(edge[0], edge[1], 1 / weight) for edge, weight in self.ref_map.items()]
        graph = nx.Graph()
        graph.add_weighted_edges_from(ebunch)
        gd = nx.node_link_data(graph, {'name': 'node'})
        self.enrich(gd)
        print(json.dumps(gd, indent=2))

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        for gn in gd['nodes']:
            node_idx = gn['node']
            variant = self.node2variant[node_idx]
            payload = self.variant2payload[variant]
            gn['doc_count'] = payload.count

    # based on https://stackoverflow.com/questions/12118720/python-tf-idf-cosine-to-find-document-similarity
    def process(self):
        vzr = TfidfVectorizer(tokenizer=retokenize, stop_words=self.stop_words)

        docs = []
        variants = self.get_doc_keys()
        for variant in variants:
            payload = self.variant2payload[variant]
            docs.append(payload.text)

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
    stop_words = load_stop_words()
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur, stop_words)
            try:
                processor.run()
                processor.process()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
