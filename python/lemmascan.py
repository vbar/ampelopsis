#!/usr/bin/python3

import csv
import os
import random
import sys
from common import get_option, get_parent_directory, make_connection
from morphodita_conv import make_tagger, retrieve_annotations
from opt_util import get_cache_path
from show_case import ShowCase

class Payload:
    def __init__(self, sample_max):
        self.sample_max = sample_max
        self.count = 0
        self.sample_list = []
        self.sample_set = set()

    def add(self, head):
        self.count += 1
        if head in self.sample_set:
            return

        if len(self.sample_list) < self.sample_max:
            self.sample_list.append(head)
            self.sample_set.add(head)
        else:
            n = random.randint(0, self.sample_max)
            if n < self.sample_max:
                doomed = self.sample_list[n]
                self.sample_list[n] = head
                self.sample_set.remove(doomed)
                self.sample_set.add(head)

class LemmaScan(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.sample_max = int(get_option("lemma_sample_max", "3"))
        self.tagger = make_tagger()
        self.qual2payload = {}

    def load_item(self, doc):
        txt = doc.get('text')
        if not txt:
            return

        bag = set()
        pair_matrix = retrieve_annotations(self.tagger, txt)
        for sentence in pair_matrix:
            for head, tail in sentence:
                payload = self.qual2payload.get(tail, None)
                if payload is None:
                    payload = Payload(self.sample_max)
                    self.qual2payload[tail] = payload

                payload.add(head)

    def dump(self):
        writer = csv.writer(sys.stdout, delimiter='\t')
        header = [ "count", "qualification" ]
        writer.writerow(header)
        for tail, payload in sorted(self.qual2payload.items(), key=lambda p: (-1 * p[1].count, p[0])):
            row = [ str(payload.count) ]
            row.append(tail)
            row.extend(sorted(payload.sample_list))
            writer.writerow(row)


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            ls = LemmaScan(cur)
            ls.run()
            ls.dump()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
