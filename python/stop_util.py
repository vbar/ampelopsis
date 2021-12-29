import os
import sys
from common import get_option, get_parent_directory

def load_stop_set():
    stop_words = set()
    cache_dir = os.path.join(get_parent_directory(), "cache")
    if not os.path.exists(cache_dir):
        raise Exception("no cache directory")

    word_list_file = os.path.join(cache_dir, "wordlist.txt")
    if not os.path.exists(word_list_file):
        raise Exception("no word list")

    print("loading words...", file=sys.stderr)
    stop_list_size = int(get_option("stop_list_size", "100"))
    with open(word_list_file) as f:
        idx = 0
        for ln in f:
            lst = ln.split()
            if lst:
                stop_words.add(lst[0])
                idx += 1
                if idx >= stop_list_size:
                    return stop_words

    return stop_words
