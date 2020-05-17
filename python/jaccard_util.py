# SciKit has it, but apparently only in a newer than installed version...
def jaccard_score(a, b):
    nom = 0
    den = 0
    l = len(a)
    assert l == len(b)
    for i in range(l):
        if a[i]:
            if b[i]:
                nom += 1

            den += 1
        elif b[i]:
            den += 1

    if not den:
        return None

    return nom / den


def weighted_jaccard_score(a, b):
    nom = 0
    den = 0
    l = len(a)
    assert l == len(b)
    for i in range(l):
        if a[i]:
            if b[i]:
                nom += a[i]
                nom += b[i]

            den += a[i]
            den += b[i]
        else:
            den += b[i]

    if not den:
        return None

    return nom / den


def set_jaccard_score(a, b):
    nom = 0
    den = 0
    l = len(a)
    assert l == len(b)
    for i in range(l):
        intersection = a[i] & b[i]
        union = a[i] | b[i]
        nom += len(intersection)
        den += len(union)

    if not den:
        return None

    return nom / den
