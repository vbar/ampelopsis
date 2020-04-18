#!/usr/bin/python3

import re
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import sys
from common import get_option, make_connection
from lang_wrap import init_lang_recog
from show_case import ShowCase
from token_util import tokenize, retokenize

class Processor(ShowCase):
    def __init__(self, cur, stop_words):
        ShowCase.__init__(self, cur)
        self.stop_words = stop_words
        self.cluster_count = int(get_option("cluster_count", "64"))
        self.lang_recog = init_lang_recog()
        self.docs = []

    def load_item(self, et):
        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            long_lst = tokenize(et['text'], True)
            txt = " ".join(long_lst)
            self.docs.append(txt)

    def process(self):
        cv = CountVectorizer(max_df=0.95, min_df=2, tokenizer=retokenize, stop_words=self.stop_words)
        df = cv.fit_transform(self.docs)
        words = cv.get_feature_names()
        lda = LatentDirichletAllocation(n_components=self.cluster_count)
        lda.fit(df)

        for index, topic in enumerate(lda.components_):
            print([words[i] for i in topic.argsort()[-15:]])


def main():
    print("loading stop words...", file=sys.stderr)
    stop_list_file = get_option("stop_list_file", "stoplist.txt")
    stop_words = []
    with open(stop_list_file) as f:
        for ln in f:
            lst = ln.split()
            if lst:
                stop_words.append(lst[0])

    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur, stop_words)
            processor.run()
            processor.process()

if __name__ == "__main__":
    main()
