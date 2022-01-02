#!/usr/bin/python3

# requires database filled by running bottle.py

import csv
import numpy as np
import os
import sys
from common import get_loose_path, get_option, make_connection
from morphodita_conv import make_tagger, tokenize_fulltext
from palette_factory import create_palette
from palette_lookup import get_membership
from person_mixin import PersonMixin
from show_case import ShowCase

class Payload:
    def __init__(self):
        self.party2count = {} # str party ID / empty string -> int count
        self.speaker_set = set() # of int speaker ID / 0

    def add_party(self, party_id):
        if not party_id:
            party_id = ""

        cnt = self.party2count.get(party_id, 0)
        self.party2count[party_id] = 1 + cnt

    def add_speaker(self, speaker_id):
        if not speaker_id:
            speaker_id = 0

        self.speaker_set.add(speaker_id)


class Processor(ShowCase, PersonMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PersonMixin.__init__(self)

        self.tagger = make_tagger()

        self.palette = create_palette(cur, "ast_party.wikidata_id")

        self.vocabulary = {} # str word -> Payload
        self.party_set = set()

    def load_item(self, doc):
        day = self.extend_date(doc)
        speech_id = doc['url_id']
        speaker_id = self.get_person(doc)

        txt = doc.get('text')
        if not txt:
            return

        party_entity = get_membership(self.palette, speaker_id, day.date())
        self.party_set.add(party_entity)

        word_set = set()
        text_matrix = tokenize_fulltext(self.tagger, txt)
        for sentence in text_matrix:
            for cased in sentence:
                word_set.add(cased.lower())

        for w in word_set:
            payload = self.vocabulary.get(w)
            if payload is None:
                payload = Payload()
                self.vocabulary[w] = payload

            payload.add_speaker(speaker_id)
            payload.add_party(party_entity)

    def dump(self, writer):
        writer.writerow(["word", "variance", "speeches", "speakers"])
        party_count = len(self.party_set)
        if not party_count:
            raise Exception("no parties found")

        word2payload = {}
        for w, payload in self.vocabulary.items():
            total = 0
            arr = []
            for _, cnt in payload.party2count.items():
                arr.append(cnt)
                total += cnt

            scale = total / 100
            scarr = []
            for n in arr:
                scarr.append(n / scale)

            while len(scarr) < party_count:
                scarr.append(0)

            word2payload[w] = (np.var(scarr), total, len(payload.speaker_set))

        for w, p in sorted(word2payload.items(), key=lambda it: (it[1], it[0])):
            writer.writerow([w, p[0], p[1], p[2]])


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            writer = csv.writer(sys.stdout, delimiter=",")
            processor.dump(writer)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
