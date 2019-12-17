import re

def make_check_rx(words):
    alts = "|".join((re.escape(w) for w in words))
    return re.compile("\\b%s\\b" % alts)

def normalize_value(v):
    if not type(v) is str:
        return None

    l = v.lower()
    return l.strip()

class TreeCheck:
    def __init__(self):
        self.spec = {} # key -> word -> ( regex matching word, entity )
        self.key2check = None # initialized lazily; key -> regex matching words under key in spec
        self.found = None # initialized before every find; set of entity

    def add(self, key, word, entity):
        word2pair = self.spec.get(key)
        if word2pair is None:
            word2pair = {}
            self.spec[key] = word2pair

        nword = normalize_value(word)
        if not nword:
            raise Exception("word %s not valid for tree check" % word)

        word2pair[nword] = (re.compile("\\b%s\\b" % re.escape(nword)), entity)

    def find(self, doc):
        if self.key2check is None:
            self.key2check = {}
            for key, word2pair in self.spec.items():
                self.key2check[key] = make_check_rx(sorted(word2pair.keys()))

        self.found = set()
        self.do_walk(doc)
        return self.found

    def do_walk(self, in_node):
        if type(in_node) is dict:
            for k, v in in_node.items():
                w = None
                if k in self.spec.keys():
                    w = normalize_value(v)
                    check_rx = self.key2check[k]
                    if w and check_rx.search(w):
                        word2pair = self.spec[k]
                        for _, p in word2pair.items():
                            if p[0].search(w):
                                self.found.add(p[1])

                if w is None:
                    self.do_walk(v)
        elif type(in_node) is list:
            for it in in_node:
                self.do_walk(it)
