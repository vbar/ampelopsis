import json
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from common import get_loose_path, get_mandatory_option

def get_detail_path(url_id):
    repre = get_mandatory_option("storage_alternative")
    path = get_loose_path(url_id, alt_repre=repre)
    return path if os.path.exists(path) else None
