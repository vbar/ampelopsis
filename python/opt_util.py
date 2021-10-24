import os
from common import get_parent_directory

def get_cache_path(name, mkdir=False):
    top_dir = os.path.abspath(get_parent_directory())
    cache_dir = os.path.join(top_dir, "cache")

    if mkdir and not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    return os.path.join(cache_dir, name)
