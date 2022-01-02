import collections
from dateutil.parser import parse
from flask import abort, Blueprint, g, jsonify, render_template
import json
import re
from .database import databased
from .filesystem import get_detail_doc
from .shared_model import list_persons

Speaker = collections.namedtuple('Speaker', 'name cards')

bp = Blueprint('detail', __name__, url_prefix='/detail')

para_rx = re.compile("\n")

def get_speaker(cur, url_id):
    cur.execute("""select presentation_name, field.url
from ast_speech
join ast_person on speaker_id=ast_person.id
left join ast_person_card on ast_person.id=ast_person_card.person_id
left join field on ast_person_card.link_id=field.id
where speech_id=%s""", (url_id,))
    rows = cur.fetchall()
    present_name = None
    cards = []
    for name, url in rows:
        if name:
            present_name = name

        if url:
            cards.append(url)

    if present_name:
        return Speaker(present_name, cards)
    else:
        return None


def get_prev_speech(cur, day, order):
    if (not day) or (order is None):
        return None

    cur.execute("""select speech_id
from ast_speech
where speech_day=%s and speech_order<%s
order by speech_order desc
limit 1""", (day, order))
    row = cur.fetchone()
    return row[0] if row else None


def get_next_speech(cur, day, order):
    if (not day) or (order is None):
        return None

    cur.execute("""select speech_id
from ast_speech
where speech_day=%s and speech_order>%s
order by speech_order
limit 1""", (day, order))
    row = cur.fetchone()
    return row[0] if row else None


def get_speech_index(cur, day, order):
    if (not day) or (order is None):
        return None

    cur.execute("""select count(*)
from ast_speech
where speech_day=%s and speech_order<%s""", (day, order))
    row = cur.fetchone()
    return row[0]


def list_day_detail(cur, dt):
    timeline = []
    cur.execute("""select speech_id, speaker_id, word_count
from ast_speech
where speech_day=%s
order by speech_order""", (dt,))
    rows = cur.fetchall()
    for row in rows:
        item = [ c for c in row ]
        timeline.append(item)

    return timeline


def get_yesterday_tomorrow(cur, dt):
    cur.execute("""select 'yesterday' k, speech_id, speech_day
from (
        select speech_id, speech_day
        from ast_speech
        where speech_day = (
                select max(speech_day)
                from ast_speech
                where speech_day<%s )
        order by speech_order desc limit 1
) yesterday
union
select 'tomorrow' k, speech_id, speech_day
from (
        select speech_id, speech_day
        from ast_speech
        where speech_day = (
                select min(speech_day)
                from ast_speech
                where speech_day>%s )
        order by speech_order limit 1
) tomorrow""", (dt, dt))
    rows = cur.fetchall()
    key2obj = {}
    for key, speech_id, speech_day in rows:
        key2obj[key] = {
            'id': speech_id,
            'day': speech_day.strftime('%-d.%-m.%Y')
        }

    return (key2obj.get('yesterday'), key2obj.get('tomorrow'))


def format_title(doc):
    return "%s_%s_%s" % (doc.get('legislature'), doc.get('session'), doc.get('order'))


def get_detail_model(cur, url_id, doc):
    raw_day = doc.get('date')
    day = None
    day_str = None
    if raw_day:
        day = parse(raw_day)
        day_str = day.strftime('%-d.%-m.%Y')

    speaker_name = None
    speaker_cards = None
    speaker = get_speaker(cur, url_id)
    if speaker:
        speaker_name = speaker.name
        speaker_cards = speaker.cards

    if not speaker_name:
        speaker_name = doc.get('speaker_name')

    txt = doc.get('text')
    if txt:
        paras = [ p for p in para_rx.split(txt) if p ]
    else:
        paras = None

    order = doc.get('order')
    model = {
        'cur_id': url_id,
        'title': format_title(doc),
        'paras': paras,
        'iso_day': day.strftime('%Y-%m-%d'),
        'day': day_str,
        'speaker_name': speaker_name,
        'prev_id': get_prev_speech(cur, day, order),
        'next_id': get_next_speech(cur, day, order),
        'index': get_speech_index(cur, day, order),
        'ext_url': doc.get('orig_url')
    }

    if (speaker_cards is not None) and len(speaker_cards):
        model['speaker_cards'] = speaker_cards

    return model


@bp.route('/<int:url_id>')
@databased
def frame(url_id):
    doc = get_detail_doc(url_id)
    if not doc:
        abort(404)

    raw_day = doc.get('date')
    day = None
    if raw_day:
        day = parse(raw_day)

    with g.conn.cursor() as cur:
        model = get_detail_model(cur, url_id, doc)
        names = list_persons(cur)

        if day:
            timeline = list_day_detail(cur, day)
            yesterday, tomorrow = get_yesterday_tomorrow(cur, day)
        else:
            timeline = None
            yesterday = None
            tomorrow = None

        return render_template('detail.html', title=doc.get('Id'), model=json.dumps(model), yesterday=yesterday, tomorrow=tomorrow, names=json.dumps(names), timeline=json.dumps(timeline))


@bp.route('/data/<int:url_id>')
@databased
def data(url_id):
    doc = get_detail_doc(url_id)
    if not doc:
        abort(404)

    with g.conn.cursor() as cur:
        model = get_detail_model(cur, url_id, doc)
        return jsonify(model)
