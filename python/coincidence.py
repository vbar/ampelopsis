#!/usr/bin/python3

import sys
from common import make_connection
from json_lookup import JsonLookup

class CoincidenceAggregator:
    def __init__(self, pri_name, sec_name):
        assert pri_name and sec_name and pri_name != sec_name
        self.pri_name = pri_name
        self.sec_name = sec_name
        self.names = None

    def walk(self, tree):
        self.names = set()
        self.do_walk(tree)
        return self.names

    def do_walk(self, in_node):
        if type(in_node) is dict:
            pri_val = None
            sec_flag = False
            for k, v in in_node.items():
                if (k == self.pri_name) and (type(v) is str):
                    pri_val = v
                    if sec_flag:
                        self.add_name(pri_val)
                elif k == self.sec_name:
                    sec_flag = True
                    if pri_val is not None:
                        self.add_name(pri_val)

                    self.do_walk(v)
                else:
                    self.do_walk(v)
        elif type(in_node) is list:
            for it in in_node:
                self.do_walk(it)

    def add_name(self, raw):
        self.names.add(raw.strip())

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            lookup = JsonLookup(cur)
            ca = CoincidenceAggregator('name', 'address')
            for a in sys.argv[1:]:
                detail = lookup.get_document(a)
                names = ca.walk(detail)
                for nm in names:
                    print(nm)

                print("")

if __name__ == "__main__":
    main()
