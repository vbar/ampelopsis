#!/usr/bin/python3

import glob
import json
import os
import sys
from common import get_parent_directory

class Accumulator:
    def __init__(self):
        self.json_dir = os.path.join(get_parent_directory(), "json")
        if not os.path.exists(self.json_dir):
            raise Exception("not found: " + self.json_dir)

        self.flag2count = {}

    def get_doc(self, json_path):
        with open(json_path) as f:
            return json.load(f)

    def accumulate(self, doc):
        statements = doc.get('statements')
        if statements and len(statements):
            flag_set = set()
            for stm in statements:
                flag = stm.get('canOpen')
                flag_set.add(str(flag))

            flags = sorted(flag_set)
            k = " / ".join(flags)
            cnt = self.flag2count.get(k, 0)
            self.flag2count[k] = cnt + 1

    def run(self):
        mask = os.path.join(self.json_dir, "*.json")
        for fname in sorted(glob.glob(mask)):
            print(os.path.basename(fname) + "...", file=sys.stderr)
            doc = self.get_doc(fname)
            self.accumulate(doc)

    def dump(self):
        flags = sorted(self.flag2count.keys(), key=lambda k: -1 * self.flag2count[k])
        for flag in flags:
            print("%d x %s" % (self.flag2count[flag], flag))


def main():
    accu = Accumulator()
    accu.run()
    accu.dump()

if __name__ == "__main__":
    main()
