from datetime import timedelta
from palette_time import add_interval

def update_started(person_obj, from_date, value):
    assert from_date

    old_started = person_obj.get('started')
    if old_started:
        if from_date < old_started[0]:
            if value == old_started[1]:
                person_obj['started'] = (from_date, value)
            else:
                # last (i.e. first-inserted) statement wins
                return
        else:
            # no improvement
            return
    else:
        person_obj['started'] = (from_date, value)

    old_ended = person_obj.get('ended')
    if old_ended and (old_ended[0] >= from_date):
        moved_until = from_date - timedelta(days=1)
        person_obj['ended'] = (moved_until, old_ended[1])


def update_ended(person_obj, until_date, value):
    assert until_date

    old_started = person_obj.get('started')
    if old_started and (old_started[0] <= until_date):
        until_date = old_started[0] - timedelta(days=1)

    old_ended = person_obj.get('ended')
    if old_ended:
        if until_date > old_ended[0]:
            if value == old_ended[1]:
                person_obj['ended'] = (until_date, value)
            # else last (i.e. first-inserted) statement wins
        # else no improvement
    else:
        person_obj['ended'] = (until_date, value)


def create_palette(cur, value_column):
    """Reads party membership from database.

Assertions about politicians and their party membership are acquired
from Wikidata (so they aren't necessarily self-consistent, let alone
correct), and even after conversion into a relational format aren't
very suitable for querying (and the web index displays too much to
query each person + date against the database individually
anyway). This function caches the party membership information from
database tables, resolving inconsistent assertions systematically (not
necessarily optimally), into a mapping of person IDs to their
time-specific party membership values - either party ID, or just its
color - that can be looked up efficiently.
    """

    palette_map = {} # int person ID -> person object
    cur.execute("""select person_id, from_date, until_date, %s as v
from ast_party_member
join ast_party on ast_party.id=party_id
order by ast_party_member.id desc""" % value_column)
    rows = cur.fetchall()
    for person_id, from_date, until_date, value in rows:
        person_obj = palette_map.setdefault(person_id, {}) # default -> str value, started -> (date from, str), ended -> (date until, str), timed -> []
        if from_date:
            if until_date:
                add_interval(person_obj, from_date, until_date, value)
            else:
                update_started(person_obj, from_date, value)
        else:
            if until_date:
                update_ended(person_obj, until_date, value)
            else:
                if 'default' not in person_obj:
                    person_obj['default'] = value

    return palette_map
