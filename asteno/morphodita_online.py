import os
import sys
from .pool import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from morphodita_conv import make_tagger, simplify_fulltext

the_pool = Pool(make_tagger)

def stem_text(txt):
    if (not txt) or (len(txt) < 2):
        return None

    with the_pool.get_resource() as tagger:
        return simplify_fulltext(tagger, set(), txt)
