from common import config

def get_quoted_list_option(name, default_value):
    if not config.has_option("root", name):
        if default_value is None or isinstance(default_value, list):
            return default_value
        else:
            raw_value = default_value
    else:
        raw_value = config.get("root", name)

    l = len(raw_value)
    if not l or (raw_value[0] != '"'):
        raise Exception("option %s has invalid value %s" % (name, raw_value))

    lst = []
    i = 1
    inside = True
    cur = ""
    while i < l:
        c = raw_value[i]
        if inside:
            if c == '"':
                lst.append(cur)
                cur = ""
                inside = False
            else:
                cur += c
        elif c == '"':
            inside = True
        elif c not in (" ", "\t"):
            raise Exception("option %s has unquoted value %s" % (name, raw_value))

        i += 1

    if inside:
        raise Exception("option %s has non-terminated value %s" % (name, raw_value))

    return lst
