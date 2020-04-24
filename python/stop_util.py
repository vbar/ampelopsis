import os
import sys
from common import get_parent_directory

def load_stop_words():
    stop_words = []
    cache_dir = os.path.join(get_parent_directory(), "cache")
    if os.path.exists(cache_dir):
        stop_list_file = os.path.join(cache_dir, "stoplist.txt")
        if os.path.exists(stop_list_file):
            print("loading stop words...", file=sys.stderr)
            with open(stop_list_file) as f:
                for ln in f:
                    lst = ln.split()
                    if lst:
                        stop_words.append(lst[0])

    return stop_words
