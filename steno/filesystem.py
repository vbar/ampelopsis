import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from common import get_loose_path

def get_detail_doc(url_id):
    path = get_loose_path(url_id)
    if not os.path.exists(path):
        return None

    with open(path, 'r') as f:
        return json.load(f)
