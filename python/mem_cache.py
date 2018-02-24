class MemCache:
    def __init__(self, high_mark, low_mark):
        assert high_mark > low_mark
        assert low_mark > 0
        self.high_mark = high_mark
        self.low_mark = low_mark
        self.cache = {} # url -> count
        
    def check(self, url):
        cnt = self.cache.get(url, 0)
        self.cache[url] = cnt + 1
        if not cnt:
            if len(self.cache) > self.high_mark:
                self.prune()

            return False
        else:
            return True

    def prune(self):
        cache = {}
        # FIXME: should use heap
        lst = sorted([ (v, k) for k, v in self.cache.items() ], reverse=True)
        for v, k in lst[:self.low_mark]:
            cache[k] = v

        self.cache = cache
