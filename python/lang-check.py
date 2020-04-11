#!/usr/bin/python3

import random
import sys
from common import get_option, make_connection
from lang import init_lang_recog
from show_case import ShowCase
from token_util import tokenize

class LangSample:
    def __init__(self, samples):
        self.samples = samples
        self.freq = 1


class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        random.seed()
        self.lang_recog = init_lang_recog()
        self.add_threshold = float(get_option("lang_mem_add_threshold", "0.001"))
        self.replace_threshold = self.add_threshold + float(get_option("lang_mem_replace_threshold", "0.5"))
        self.lang2sample = {} # str -> LangSample

    def load_item(self, et):
        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if not lng:
            lng = 'other'

        sample = self.lang2sample.get(lng)
        if not sample:
            self.lang2sample[lng] = LangSample(samples=[et])
        else:
            sample.freq += 1
            r = random.random()
            if r < self.add_threshold:
                sample.samples.append(et)
            elif r < self.replace_threshold:
                d = random.randrange(len(sample.samples))
                del sample.samples[d]
                sample.samples.append(et)

    def dump(self):
        items = sorted(self.lang2sample.items(), key=lambda p: (-1 * p[1].freq, p[0]))
        for lng, sample in items:
            print(lng, "\t", sample.freq)

        for lng, sample in items:
            print("")
            print(lng, "\t", len(sample.samples))
            for et in sample.samples:
                print(et['url'])
                print(et['text'])


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
            processor.dump()


if __name__ == "__main__":
    main()
