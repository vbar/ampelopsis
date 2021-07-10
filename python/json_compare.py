import json

def stringify(obj):
    return json.dumps(obj, sort_keys=True)

# https://www.quora.com/How-do-I-compare-two-JSON-files-in-Python

def compare_object(a, b):
    ta = type(a)
    if ta != type(b):
        return False
    elif ta is dict:
        return compare_dict(a, b)
    elif ta is list:
        return compare_list(a, b)
    else:
        return a == b

def compare_dict(a, b):
    if len(a) != len(b):
        return False
    else:
        for k, v in a.items():
            if not k in b:
                return False
            elif not compare_object(v, b[k]):
                return False

        return True

def compare_list(a, b):
    la = len(a)
    if la != len(b):
        return False
    else:
        sa = sorted(a, key=stringify)
        sb = sorted(b, key=stringify)
        for i in range(la):
            if not compare_object(sa[i], sb[i]):
                return False

        return True
