from cursor_wrapper import CursorWrapper

class PathBuilder(CursorWrapper):
    def __init__(self, cur, high_mark=0, low_mark=0):
        CursorWrapper.__init__(self, cur)
        self.high_mark = high_mark
        self.low_mark = low_mark
        if (high_mark > low_mark) and (low_mark > 0):
            self.cache = {}
            self.get_parents = self.get_parents_memo
        else:
            self.get_parents = self.get_parents_simple
            
    def get_parents_memo(self, urls, child_depth):
        key_list = [ child_depth ]
        key_list.extend(urls)
        key = tuple(key_list)
        value = self.cache.get(key, None)
        if value is None:
            parents = self.get_parents_simple(urls, child_depth)
            value = [ 1 ]
            value.extend(parents)
            self.cache[key] = value
                
            if len(self.cache) > self.high_mark:
                self.prune()
        else:
            value[0] += 1
            parents = value[1:]

        return parents
        
    def get_parents_simple(self, urls, child_depth):            
        self.cur.execute("""select distinct from_id
from edges
join nodes on from_id=url_id
where to_id in %s and depth=%s
order by from_id""", (tuple(urls), child_depth - 1))
        rows = self.cur.fetchall()
        return [ row[0] for row in rows ]

    def prune(self):
        cache = {}
        # FIXME: should use heap
        lst = sorted([ (v, k) for k, v in self.cache.items() ], reverse=True)
        for v, k in lst[:self.low_mark]:
            cache[k] = v

        self.cache = cache
        
