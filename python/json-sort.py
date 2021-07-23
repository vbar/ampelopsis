#!/usr/bin/python3

import json
import sys
from json_compare import stringify

def canonicalize(obj):
    t = type(obj)
    if t is dict:
        canonicalize_dict(obj)
    elif t is list:
        canonicalize_list(obj)


def canonicalize_dict(obj):
    for k, v in obj.items():
        # JSON keys cannot be composite
        canonicalize(v)


def canonicalize_list(obj):
    obj.sort(key=stringify)
    for it in obj:
        canonicalize(it)


def main():
    for fname in sys.argv[1:]:
        with open(fname) as f:
            doc = json.load(f)
            canonicalize(doc)

        s = json.dumps(doc, indent=4, sort_keys=True)
        with open(fname, 'w') as f:
            print(s, file=f)

if __name__ == "__main__":
    main()
