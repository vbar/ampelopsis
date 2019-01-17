import re

university_alternatives = "univerzita|učení|škola"

university_name_rx = re.compile("\\b(?:" + university_alternatives + ")\\b")

school_name_rx = re.compile("\\b(?:" + university_alternatives + "|zš|gymn[aá][sz]ium)\\b")

def get_org_name(it):
    mixed = it['organization'].strip()
    return mixed.lower()

def convert_answer_to_iterable(answer, it):
    if callable(answer): # technically we could have a cycle, but hopefully nobody will need that...
        answer = answer(it)

    if isinstance(answer, str):
        return (answer,)
    else: # must be iterable
        return answer
