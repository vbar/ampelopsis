#!/usr/bin/python3

# requires download with funnel_links set (to at least 1) and database
# filled by running condensate.py

import collections
import datetime
from math import sqrt
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import sys
from analyzer import Analyzer
from common import get_option, make_connection
from distance_args import ConfigArgs
from jaccard_util import set_jaccard_score
from pinhole_base import PinholeBase
from stem_recon import reconstitute
from stop_util import load_stop_words
from timeline_helper_mixin import TimelineHelperMixin
from token_util import tokenize

Occurence = collections.namedtuple('Occurence', 'time_bucket url_id')

class Processor(PinholeBase, TimelineHelperMixin):
    def __init__(self, cur, stop_words):
        PinholeBase.__init__(self, cur, False, '*')
        TimelineHelperMixin.__init__(self, get_option("timeline_bin_scale", "minutes"))
        self.puff = int(get_option("event_distance_puff", "5"))
        self.link_threshold = float(get_option("inverse_distance_threshold", "0.01"))
        self.cluster_count = int(get_option("cluster_count", "128"))
        self.topic_match_threshold = float(get_option("topic_match_threshold", "0.67"))
        self.stop_words = stop_words
        self.urlid2doc = {}
        self.urlid2row = None
        self.matrix = None
        self.key2timeline = {} # hamlet name -> list of Occurence
        self.now_sorted = True # empty is sorted
        self.value_series = None # opt hamlet name -> list of (frozen)set of int topic
        self.terrain = {} # source hamlet name -> target hamlet name -> count
        self.hamlet2count = {}
        # actually seems to work better w/o stemming...
        self.reconstitute = self.reconstitute_rect if get_option("use_stemmed", True) else self.reconstitute_simple

    def load_item(self, et):
        url = et['url']
        url_id = self.get_url_id(url)
        if not url_id:
            return

        txt = self.reconstitute(et)
        if not txt:
            return

        dt = self.extend_date(et)
        hamlet_name = et['osobaid']
        self.urlid2doc[url_id] = txt
        self.add_sample(hamlet_name, dt, url_id)

    def reconstitute_rect(self, et):
        return reconstitute(self.cur, et['url'])

    def reconstitute_simple(self, et):
        lst = tokenize(et['text'], True)
        return " ".join(lst)

    def enrich(self, gd):
        PinholeBase.enrich(self, gd)

        for gn in gd['nodes']:
            node_idx = gn['node']
            hamlet_name = self.node2variant[node_idx]
            series = self.value_series[hamlet_name]
            gn['doc_count'] = sum((len(s) for s in series))

    def process(self):
        print("computing topics...", file=sys.stderr)

        self.urlid2row = {}
        cv = CountVectorizer(max_df=0.95, min_df=2, analyzer=Analyzer(self.stop_words))
        docs = []
        for url_id, doc in sorted(self.urlid2doc.items()):
            self.urlid2row[url_id] = len(docs)
            docs.append(doc)

        df = cv.fit_transform(docs)
        lda = LatentDirichletAllocation(n_components=self.cluster_count)
        lda.fit(df)
        self.matrix = lda.transform(df)

        print("computing event correlation...", file=sys.stderr)

        self.lazy_model()

        persons = []
        setmatrix = []
        for hamlet_name, series in sorted(self.value_series.items()):
            persons.append(hamlet_name)
            setmatrix.append(series)

        l = len(setmatrix)
        for i in range(l):
            for j in range(i + 1, l):
                print("measuring similarity between %s and %s..." % (persons[i], persons[j]), file=sys.stderr)
                sim = set_jaccard_score(setmatrix[i], setmatrix[j])
                if (sim is not None) and (sim > self.link_threshold):
                    # hamlet name is-a variant
                    low_node = self.introduce_node(persons[i], False)
                    high_node = self.introduce_node(persons[j], False)
                    edge = (low_node, high_node)
                    self.ref_map[edge] = sqrt(1 / sim)

    def add_sample(self, hamlet_name, rdt, url_id):
        dt = self.quantize(rdt)

        timeline = self.key2timeline.get(hamlet_name)
        if not timeline:
            timeline = []
            self.key2timeline[hamlet_name] = timeline

        timeline.append(Occurence(dt, url_id))

        # inspired by http://dl.ifip.org/db/conf/im/im2019-ws1-annet/191658.pdf
        if self.puff > 0:
            delta = self.get_step()
            before = dt
            after = dt
            for i in range(self.puff):
                before -= delta
                timeline.append(Occurence(before, url_id))
                after += delta
                timeline.append(Occurence(after, url_id))

        self.now_sorted = False

    def lazy_model(self):
        if self.value_series is not None:
            return

        self.lazy_sort()

        self.value_series = {}
        delta = self.get_step()
        dt = self.get_min_date()
        maxdt = self.get_max_date()
        idx_map = {}
        while dt <= maxdt:
            for hamlet_name, timeline in self.key2timeline.items(): # now sorted
                l = len(timeline)
                idx = idx_map.get(hamlet_name, 0)
                matchset = set()
                while (idx < l) and (dt == timeline[idx].time_bucket):
                    url_id = timeline[idx].url_id
                    i = self.urlid2row[url_id]
                    for j in range(len(self.matrix[i])):
                        if self.matrix[i][j] >= self.topic_match_threshold:
                            matchset.add(j)

                    idx += 1

                vseries = self.value_series.get(hamlet_name)
                if not vseries:
                    vseries = []
                    self.value_series[hamlet_name] = vseries

                vseries.append(frozenset(matchset))

                idx_map[hamlet_name] = idx

            dt += delta

    def get_min_date(self):
        return self.quantize(self.mindate)

    def get_max_date(self):
        return self.quantize(self.maxdate)

    def lazy_sort(self):
        if self.now_sorted:
            return

        key2timeline = {}
        for hamlet_name, timeline in self.key2timeline.items():
            key2timeline[hamlet_name] = sorted(timeline, key=lambda o: o.time_bucket)

        self.key2timeline = key2timeline
        self.now_sorted = True


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
