from bisect import bisect_left

def get_membership(palette, person_id, day):
    if not person_id:
        return None

    person = palette.get(person_id)
    if not person:
        return None

    timed = person.get('timed')
    if timed:
        i = bisect_left(timed, [ day ])
        if i < len(timed):
            stop = timed[i]

            # the search above actually doesn't search for from stops,
            # but for something a bit smaller...
            if (i + 1) < len(timed):
                next_stop = timed[i + 1]
                if next_stop[0] == day:
                    assert len(next_stop) == 3
                    return next_stop[2]

            if stop[0] == day:
                if len(stop) == 1:
                    assert i > 0
                    stop = timed[i - 1]

                assert len(stop) == 3
                return stop[2]
            elif len(stop) == 1:
                assert i > 0
                stop = timed[i - 1]
                return stop[2]

    started = person.get('started')
    if started and (day >= started[0]):
        return started[1]

    ended = person.get('ended')
    if ended and (day <= ended[0]):
        return ended[1]

    return person.get('default')
