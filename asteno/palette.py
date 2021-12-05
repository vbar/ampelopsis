from datetime import timedelta
from flask import Blueprint, g, jsonify
from .database import databased
from .timeline import add_interval

bp = Blueprint('palette', __name__, url_prefix='/palette')

def update_started(person_obj, from_date, color):
    assert from_date

    old_started = person_obj.get('started')
    if old_started:
        if from_date < old_started[0]:
            if color == old_started[1]:
                person_obj['started'] = (from_date, color)
            else:
                # last (i.e. first-inserted) statement wins
                return
        else:
            # no improvement
            return
    else:
        person_obj['started'] = (from_date, color)

    old_ended = person_obj.get('ended')
    if old_ended and (old_ended[0] >= from_date):
        moved_until = from_date - timedelta(days=1)
        person_obj['ended'] = (moved_until, old_ended[1])


def update_ended(person_obj, until_date, color):
    assert until_date

    old_started = person_obj.get('started')
    if old_started and (old_started[0] <= until_date):
        until_date = old_started[0] - timedelta(days=1)

    old_ended = person_obj.get('ended')
    if old_ended:
        if until_date > old_ended[0]:
            if color == old_ended[1]:
                person_obj['ended'] = (until_date, color)
            # else last (i.e. first-inserted) statement wins
        # else no improvement
    else:
        person_obj['ended'] = (until_date, color)


def serialize_color(c):
    return '#' + c


def serialize_date(d):
    return d.strftime('%Y-%m-%d')


def serialize_half_bounded(interval):
    assert len(interval) == 2
    return [ serialize_date(interval[0]), serialize_color(interval[1]) ]


def serialize_stop(s):
    l = len(s)
    assert l in (1, 3)
    lst = [ serialize_date(s[0]) ]
    if l == 3:
        lst.append(serialize_date(s[1]))
        lst.append(serialize_color(s[2]))

    return lst


def serialize_person(person_obj):
    person_plain = {}
    dflt = person_obj.get('default')
    if dflt:
        person_plain['default'] = serialize_color(dflt)

    started = person_obj.get('started')
    ended = person_obj.get('ended')
    if started and ended and (started[1] == ended[1]) and ((started[0] - timedelta(days=1)) == ended[0]):
        person_plain['default'] = serialize_color(started[1])
    else:
        if started:
            person_plain['started'] = serialize_half_bounded(started)

        if ended:
            person_plain['ended'] = serialize_half_bounded(ended)

    time_list = person_obj.get('timed')
    if time_list:
        person_plain['timed'] = [ serialize_stop(s) for s in time_list ]

    return person_plain


@bp.route("/")
@databased
def data():
    palette_map = {} # int person ID -> person object
    with g.conn.cursor() as cur:
        cur.execute("""select person_id, from_date, until_date, coalesce(color, 'AAA') as clr
from ast_party_member
join ast_party on ast_party.id=party_id
order by ast_party_member.id desc""")
        rows = cur.fetchall()
        for person_id, from_date, until_date, color in rows:
            person_obj = palette_map.setdefault(person_id, {}) # default -> str color, started -> (date from, str), ended -> (date until, str), timed -> []
            if from_date:
                if until_date:
                    add_interval(person_obj, from_date, until_date, color)
                else:
                    update_started(person_obj, from_date, color)
            else:
                if until_date:
                    update_ended(person_obj, until_date, color)
                else:
                    if 'default' not in person_obj:
                        person_obj['default'] = color

    palette_out = {}
    for person_id, person_obj in sorted(palette_map.items()):
        palette_out[str(person_id)] = serialize_person(person_obj)

    return jsonify(palette_out)
