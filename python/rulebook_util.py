import re

school_name_rx = re.compile("\\b(?:univerzita|učení|škola)")

def get_org_name(it):
    mixed = it['organization'].strip()
    return mixed.lower()
