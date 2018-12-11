#!/usr/bin/python3

import sys
import timeit
from Levenshtein import distance

class Corrector:
    def __init__(self, n, di, debug_name=""):
        if n < 1:
            raise Exception("max number of corrections must be positive")

        self.n = n
        self.ds = set(di)
        self.debug_name = debug_name
        if debug_name:
            self.call_count = 0
            self.total_time = 0

    def is_correct(self, w):
        return w in self.ds

    def match(self, w):
        if self.debug_name:
            self.call_count += 1
            start = timeit.default_timer()

        m = set()
        for k in self.ds:
            if distance(w, k) <= self.n:
                m.add(k)

        if self.debug_name:
            self.total_time += timeit.default_timer() - start

        return m

    # only callable for objects instantiated with non-empty debug_name
    def debug_dump(self):
        print("%s: %.2f seconds in %d calls" % (self.debug_name, self.total_time, self.call_count))

if __name__ == "__main__":
    print("OK")
