from bisect import bisect_left
from datetime import timedelta

# timeline / time_list is a list of stops. stop is an array of 1 or 3
# elements, where the 0-th element is a (from or until) date (with day
# precision). Iff the date is a from date, it is followed (in the
# stop) by another (until) date and str color. Time intervals are
# closed; interval longer than a day is represented by a pair of from
# date stop and until date stop while 1-day time interval has only the
# from date stop. stops in timeline are ordered by the 0-th date,
# which is unique (at 0-th index of all spots of a timeline).

def insert_interval(time_list, idx, from_date, until_date, color):
    if from_date > until_date:
        raise Exception("invalid bounds")

    from_stop = [ from_date, until_date, color ]
    time_list.insert(idx, from_stop)

    if from_date < until_date:
        until_stop = [ until_date ]
        time_list.insert(idx + 1, until_stop)


def merge_after(time_list, begin_stop, j, from_date, until_date, color):
    if j < len(time_list):
        old_stop = time_list[j]
        assert len(old_stop) == 3
    else:
        old_stop = None

    while old_stop and (until_date >= old_stop[0]) and (color == old_stop[2]):
        del time_list[j]
        if old_stop[0] < old_stop[1]:
            del time_list[j]

        if j < len(time_list):
            old_stop = time_list[j]
            assert len(old_stop) == 3
        else:
            old_stop = None

    if old_stop and (until_date >= old_stop[0]):
        until_date = old_stop[0] - timedelta(days=1)

    if until_date > begin_stop[1]:
        begin_stop[1] = until_date
    else:
        until_date = begin_stop[1]

    if from_date < until_date:
        until_stop = [ until_date ]
        time_list.insert(j, until_stop)
    else:
        assert from_date == until_date


def add_before_from(time_list, i, from_date, until_date, color):
    found_stop = time_list[i]
    assert (len(found_stop) == 3) and (from_date < found_stop[0])

    if until_date < found_stop[0]:
        # there is a gap
        insert_interval(time_list, i, from_date, until_date, color)
    else:
        if color == found_stop[2]:
            # can merge (at least 1)
            j = i + 1
            if found_stop[0] < found_stop[1]:
                assert j < len(time_list)
                del time_list[j]

            found_stop[0] = from_date
            merge_after(time_list, found_stop, j, from_date, until_date, color)
        else:
            # cannot merge
            until_date = found_stop[0] - timedelta(days=1)
            insert_interval(time_list, i, from_date, until_date, color)


def add_before_until(time_list, i, from_date, until_date, color):
    found_stop = time_list[i]
    assert (i > 0) and (len(found_stop) == 1)

    begin_stop = time_list[i - 1]
    assert len(begin_stop) == 3
    if from_date > begin_stop[0]:
        if color == begin_stop[2]:
            # can merge (at least 1)
            del time_list[i]
            begin_stop[0] = from_date
            merge_after(time_list, begin_stop, i, from_date, until_date, color)
        else:
            # cannot merge
            new_until = from_date - timedelta(days=1)
            begin_stop[1] = new_until
            if begin_stop[0] < begin_stop[1]:
                found_stop[0] = new_until
                k = i + 1
            else:
                assert begin_stop[0] == begin_stop[1]
                del time_list[i]
                k = i

            if k < len(time_list):
                add_before_from(time_list, k, from_date, until_date, color)
            else:
                assert k == len(time_list)
                insert_interval(time_list, k, from_date, until_date, color)
    else:
        assert from_date == begin_stop[0]
        if color == begin_stop[2]:
            del time_list[i]
            merge_after(time_list, begin_stop, i, from_date, until_date, color)
        # else last (i.e. first-inserted) statement wins


def add_at_from(time_list, i, from_date, until_date, color):
    found_stop = time_list[i]
    assert (len(found_stop) == 3) and (from_date == found_stop[0])

    if color == found_stop[2]:
        # can merge (at least 1)
        j = i + 1
        if found_stop[0] < found_stop[1]:
            assert j < len(time_list)
            del time_list[j]

        merge_after(time_list, found_stop, j, from_date, until_date, color)
    # else last (i.e. first-inserted) statement wins


def add_at_until(time_list, i, from_date, until_date, color):
    found_stop = time_list[i]
    assert (i > 0) and (len(found_stop) == 1) and (from_date == found_stop[0])

    begin_stop = time_list[i - 1]
    assert (len(begin_stop) == 3) and (begin_stop[0] < begin_stop[1])

    if color == begin_stop[2]:
        # can merge (at least 1)
        del time_list[i]
        merge_after(time_list, begin_stop, i, from_date, until_date, color)
    # else last (i.e. first-inserted) statement wins


def add_interval(person_obj, from_date, until_date, color):
    assert from_date
    assert until_date

    time_list = person_obj.setdefault('timed', [])
    planned_stop = [ from_date, until_date, color ]
    i = bisect_left(time_list, planned_stop)
    if i == len(time_list):
        insert_interval(time_list, i, from_date, until_date, color)
    else:
        found_stop = time_list[i]
        if len(found_stop) == 3:
            # found from (possibly 1-day interval)
            assert found_stop[0] <= found_stop[1]
            if from_date < found_stop[0]:
                add_before_from(time_list, i, from_date, until_date, color)
            else:
                assert from_date == found_stop[0]
                add_at_from(time_list, i, from_date, until_date, color)
        else:
            # found until
            assert (i > 0) and (len(found_stop) == 1)
            if from_date < found_stop[0]:
                add_before_until(time_list, i, from_date, until_date, color)
            else:
                assert from_date == found_stop[0]
                add_at_until(time_list, i, from_date, until_date, color)
