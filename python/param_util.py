from urllib.parse import parse_qs, urlencode

def get_param_set(query):
    # we could just split query on '&', but this handles duplicate
    # names and hopefully other edge cases as well
    qd = parse_qs(query)
    params = set()
    for kv in qd.items():
        params.add(urlencode([kv], True))

    return params
