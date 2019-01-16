import re

university_alternatives = "univerzita|učení|škola"

university_name_rx = re.compile("\\b(?:" + university_alternatives + ")\\b")

school_name_rx = re.compile("\\b(?:" + university_alternatives + "|zš)\\b")

def get_org_name(it):
    mixed = it['organization'].strip()
    return mixed.lower()
