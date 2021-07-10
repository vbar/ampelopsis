#!/usr/bin/python3

import glob
import json
import os
import sys
from common import get_parent_directory
from json_compare import compare_object

class Comparer:
    def __init__(self):
        self.old_json_dir = os.path.join(get_parent_directory(), "old_json")
        if not os.path.exists(self.old_json_dir):
            raise Exception("not found: " + self.old_json_dir)

        self.new_json_dir = os.path.join(get_parent_directory(), "json")
        if not os.path.exists(self.new_json_dir):
            raise Exception("not found: " + self.new_json_dir)

        self.doc_gain = 0
        self.doc_loss = 0
        self.link_gain = 0
        self.link_loss = 0
        self.link_change = 0
        self.other_change = 0

    @staticmethod
    def get_names(json_dir):
        mask = os.path.join(json_dir, "*.json")
        return sorted([os.path.basename(n) for n in glob.glob(mask)])

    @staticmethod
    def get_doc(json_path):
        with open(json_path) as f:
            return json.load(f)

    @staticmethod
    def cond_print(msg_fmt, count):
        if count:
            print(msg_fmt % count)

    def run(self):
        old_names = Comparer.get_names(self.old_json_dir)
        old_len = len(old_names)
        i = 0
        new_names = Comparer.get_names(self.new_json_dir)
        new_len = len(new_names)
        j = 0
        while (i < old_len) and (j < new_len):
            od = old_names[i]
            nd = new_names[j]
            if od < nd:
                self.doc_loss += 1
                print("no doc " + od, file=sys.stderr)
                i += 1
            elif od > nd:
                self.doc_gain += 1
                print("new doc " + nd, file=sys.stderr)
                j += 1
            else:
                self.compare(od)
                i += 1
                j += 1

        self.doc_loss += (old_len - i)
        while i < old_len:
            od = old_names[i]
            print("no doc " + od, file=sys.stderr)
            i += 1

        self.doc_gain += (new_len - j)
        while j < new_len:
            nd = new_names[j]
            print("new doc " + nd, file=sys.stderr)
            j += 1

    def dump(self):
        print("")
        Comparer.cond_print("missing %d document(s)", self.doc_loss)
        Comparer.cond_print("%d new document(s)", self.doc_gain)
        Comparer.cond_print("%d wikidata link(s) changed", self.link_change)
        Comparer.cond_print("missing %d wikidata link(s)", self.link_loss)
        Comparer.cond_print("%d new wikidata link(s)", self.link_gain)
        Comparer.cond_print("%d other change(s)", self.other_change)

    def compare(self, doc_name):
        old_doc = Comparer.get_doc(os.path.join(self.old_json_dir, doc_name))
        new_doc = Comparer.get_doc(os.path.join(self.new_json_dir, doc_name))
        old_wid = old_doc.get('wikidataId')
        new_wid = new_doc.get('wikidataId')
        if old_wid:
            if new_wid:
                if old_wid != new_wid:
                    print("wikidata link changed for " + doc_name, file=sys.stderr)
                    self.link_change += 1
                    return
            else:
                print("no wikidata link for " + doc_name, file=sys.stderr)
                self.link_loss += 1
                return
        else:
            if new_wid:
                print("new wikidata link for " + doc_name, file=sys.stderr)
                self.link_gain += 1
                return

        if not compare_object(old_doc, new_doc):
            print("other change for " + doc_name, file=sys.stderr)
            self.other_change += 1


def main():
    comparer = Comparer()
    comparer.run()
    comparer.dump()

if __name__ == "__main__":
    main()
