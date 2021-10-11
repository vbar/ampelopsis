import numpy as np
import random
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import sys
from analyzer import Analyzer
from common import get_option
from lang_wrap import init_lang_recog
from show_case import ShowCase
from stem_mixin import StemMixin
from token_util import tokenize

class DirichletBase(ShowCase, StemMixin):
    def __init__(self, cur, stop_words):
        ShowCase.__init__(self, cur)
        StemMixin.__init__(self)
        random.seed()
        self.stop_words = stop_words
        self.cluster_count = int(get_option("cluster_count", "128"))
        self.visible_clusters = int(get_option("visible_clusters", "128"))
        self.lang_recog = init_lang_recog()
        self.url2doc = {}
        self.url2person = {}
        self.topics = []
        self.matrix = None

    def load_item(self, et):
        if self.is_redirected(et['url']):
            return

        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
            url = self.get_circuit_url(et['url'])
            self.url2person[url] = et['osobaid']
            self.load_doc(et)

    def process(self):
        print("computing topics...", file=sys.stderr)

        cv = CountVectorizer(max_df=0.95, min_df=2, analyzer=Analyzer(self.stop_words))
        docs = [ doc for url, doc in sorted(self.url2doc.items(), key=lambda p: p[0]) ]
        if not len(docs): # seen w/ incorrect download config
            raise Exception("No input documents found - check filtering conditions in load_item.")

        df = cv.fit_transform(docs)
        words = cv.get_feature_names()
        lda = LatentDirichletAllocation(n_components=self.cluster_count, learning_method='online')
        lda.fit(df)

        for index, topic in enumerate(lda.components_):
            top = "\n".join([words[i] for i in topic.argsort()[-4:]])
            self.topics.append(top)

        self.matrix = lda.transform(df)

    def sample_topics(self):
        l = len(self.topics)
        if self.visible_clusters < l:
            print("sampling %d of %d topics..." % (self.visible_clusters, l), file=sys.stderr)
            supplementary_threshold = float(self.visible_clusters) / float(l)
            wide = self.matrix
            assert l == wide.shape[1]
            narrow = None
            topics = []
            curve = []
            for i in range(l):
                curve.append(self.get_prominence(wide[:, i:i+1]))

            curve.sort(reverse=True)
            threshold = curve[self.visible_clusters]
            for i in range(l):
                col = wide[:, i:i+1]
                mx = self.get_prominence(col)
                if (mx > threshold) or ((mx == threshold) and (random.random() < supplementary_threshold)):
                    if narrow is None:
                        narrow = np.array(col, copy=True)
                    else:
                        narrow = np.concatenate((narrow, col), axis=1)

                    topics.append(self.topics[i])
                    if len(topics) >= self.visible_clusters:
                        break

            assert narrow.shape[1] == len(topics)
            self.matrix = narrow.tolist()
            self.topics = topics

    def get_prominence(self, col):
        person2max = {}
        for i, url in enumerate(self.get_urls()):
            v = col[i]
            person = self.url2person[url]
            ov = person2max.get(person, 0)
            if v > ov:
                person2max[person] = v

        return sum(person2max.values())

    def get_urls(self):
        return sorted(self.url2doc.keys())
