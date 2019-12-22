import re

university_alternatives = "univerzita|učení|škola"

university_name_rx = re.compile("\\b(?:" + university_alternatives + ")\\b")

school_name_rx = re.compile("(.*?\\b(?:" + university_alternatives + "|akademie|zš|gymn[aá][sz]ium)(?:$| \\w+))")

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

def reduce_substrings_to_shortest(str_set):
    l = len(str_set)
    if l <= 1:
        return str_set

    str_list = sorted(str_set, key=lambda s: (len(s), s))
    black = set()
    i = 0
    while i < l - 1: # the longest string isn't a substring of any other
        shrt = str_list[i]
        j = i + 1
        while j < l:
            cand = str_list[j]
            if shrt in cand:
                black.add(cand) # reject

            j += 1

        i += 1

    return str_set - black
