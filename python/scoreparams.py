#!/usr/bin/python3

from urllib.parse import urlparse, urlunparse
from common import get_netloc, make_connection
from cursor_wrapper import CursorWrapper
from param_util import get_param_set

class Builder(CursorWrapper):
    def __init__(self, cur):
        CursorWrapper.__init__(self, cur)
        self.eq_map = {} # ( url_head, url_id... ) -> [ url_id ]; length of list >= 2
        
    def prepare(self):
        print("checking equivalence classes...")
        self.cur.execute("""select from_set, to_set
from edge_sets
where array_length(from_set, 1) > 1""")
        rows = self.cur.fetchall()
        for row in rows:
            self.prepare_class(*row)
            
    def process(self):
        print("got %d non-trivial equivalence classes" % (len(self.eq_map.keys())))
        idx = 0
        for key, urls in self.eq_map.items():
            l = len(urls)
            i = 0
            while i < l:
                j = i + 1
                while j < l:
                    self.classify(key[0], urls[i], urls[j])
                    j += 1

                i += 1
                
            idx += 1
            if not (idx % 10000):
                print("class %d..." % (idx,))
                
    def prepare_class(self, parents, children):
        digest_map = {}
        for url_id in parents:
            url = self.get_url(url_id)
            pr = urlparse(url)
            head_pr = (pr.scheme, get_netloc(pr), pr.path, pr.params, '', '')
            url_head = urlunparse(head_pr)
            if url_head not in digest_map:
                digest_map[url_head] = [ url_id ]
            else:
                digest_map[url_head].append(url_id)
            
        for url_head, urls in digest_map.items():
            if len(urls) > 1:
                lst = [ url_head ]
                lst.extend(children)
                key = tuple(lst)
                assert key not in self.eq_map
                self.eq_map[key] = urls

    def get_params(self, url_id):
        url = self.get_url(url_id)
        pr = urlparse(url)
        return get_param_set(pr.query)
    
    def classify(self, url_head, id1, id2):
        params1 = self.get_params(id1)
        params2 = self.get_params(id2)
        if params1 == params2:
            # normally this doesn't happen; if some site has (many)
            # such URLs, it's a problem that'll have to be handled
            # when it occurs
            print("same parameters in multiple URLs starting with " + url_head)
        else:
            all_params = set()
            all_params.update(params1)
            all_params.update(params2)
            for param in all_params:
                self.classify_param(param, param in params1, param in params2)

    def classify_param(self, param, in1, in2):
        assert in1 or in2

        agreement = 0
        irrelevancy = 0
        if in1 == in2:
            agreement = 1
        else:
            irrelevancy = 1

        self.cur.execute("""insert into param_scoreboard(nameval, agreement, irrelevancy)
values(%s, %s, %s)
on conflict(nameval) do update
set agreement=param_scoreboard.agreement + %s, irrelevancy=param_scoreboard.irrelevancy + %s""", (param, agreement, irrelevancy, agreement, irrelevancy))


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            builder = Builder(cur)
            builder.prepare()
            builder.process()
                
if __name__ == "__main__":
    main()
