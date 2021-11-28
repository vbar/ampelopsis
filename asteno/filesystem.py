import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from common import get_loose_path, get_option

def get_detail_doc(url_id):
    path = get_loose_path(url_id)
    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        doc = json.load(f)

    plain_repre = get_option("plain_repre", "plain")
    plain_path = get_loose_path(url_id, alt_repre=plain_repre)
    if os.path.exists(plain_path):
        with open(plain_path, 'r') as f:
            txt = f.read()
            doc['text'] = txt

    return doc
