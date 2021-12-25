from datetime import timedelta
from flask import Blueprint, g, jsonify
import os
import sys
from .database import databased

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from palette_factory import create_palette

bp = Blueprint('palette', __name__, url_prefix='/palette')

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
    with g.conn.cursor() as cur:
        palette_map = create_palette(cur, "coalesce(ast_party.color, 'AAA')")

    palette_out = {}
    for person_id, person_obj in sorted(palette_map.items()):
        palette_out[str(person_id)] = serialize_person(person_obj)

    return jsonify(palette_out)
