import re

datetime_rx = re.compile('^([0-9]{4})-[0-9]{2}-[0-9]{2}T00:00:00Z$')

def get_opt(it, vn):
    d = it.get(vn)
    return d.get('value') if d else None


def birth_check(person, it):
    raw_date = get_opt(it, 'b')
    if not raw_date:
        # if the date isn't in response, it must have been filtered on
        # the server
        return True

    m = datetime_rx.match(raw_date)
    if not m:
        # shouldn't happen; in case of wikidata error, take the name
        # as sufficient
        return True

    year = int(m.group(1))
    return person.birth_year == year


def get_from_year(it):
    raw_date = get_opt(it, 'f')
    if not raw_date:
        return None

    m = datetime_rx.match(raw_date)
    if not m:
        # shouldn't happen; in case of wikidata error, treat the value
        # as missing (i.e. fail for multiple parties)
        return None

    return int(m.group(1))
