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
        self.stop_words = stop_words
        self.cluster_count = int(get_option("cluster_count", "128"))
        self.lang_recog = init_lang_recog()
        self.url2doc = {}
        self.topics = []
        self.matrix = None

    def load_item(self, et):
        if self.is_redirected(et['url']):
            return

        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng == 'cs':
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

    def get_urls(self):
        return sorted(self.url2doc.keys())
