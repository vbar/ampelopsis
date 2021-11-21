import re

split_rx = re.compile('\\W+')

def tokenize(raw):
    lst = (sw for sw in split_rx.split(raw))
    return [w.lower() for w in lst]
