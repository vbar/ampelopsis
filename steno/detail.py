import collections
from dateutil.parser import parse
from flask import abort, Blueprint, g, jsonify, render_template
import json
from .database import databased
from .filesystem import get_detail_doc

Speaker = collections.namedtuple('Speaker', 'name card')

bp = Blueprint('detail', __name__, url_prefix='/detail')

def get_speaker(cur, url_id):
    cur.execute("""select presentation_name, field.url
from steno_speech
join steno_record on speaker_id=steno_record.id
left join field on card_url_id=field.id
where speech_id=%s""", (url_id,))
    row = cur.fetchone()
    if row:
        return Speaker(row[0], row[1])
    else:
        return None


def get_prev_speech(cur, day, order):
    if (not day) or (order is None):
        return None

    cur.execute("""select speech_id
from steno_speech
where speech_day=%s and speech_order<%s
order by speech_order desc
limit 1""", (day, order))
    row = cur.fetchone()
    return row[0] if row else None


def get_next_speech(cur, day, order):
    if (not day) or (order is None):
        return None

    cur.execute("""select speech_id
from steno_speech
where speech_day=%s and speech_order>%s
order by speech_order
limit 1""", (day, order))
    row = cur.fetchone()
    return row[0] if row else None


def get_detail_model(cur, url_id, doc):
    raw_day = doc.get('datum')
    day = None
    day_str = None
    if raw_day:
        day = parse(raw_day)
        day_str = day.strftime('%-d.%-m.%Y')

    speaker_name = None
    speaker_card = None
    speaker = get_speaker(cur, url_id)
    if speaker:
        speaker_name = speaker.name
        speaker_card = speaker.card

    if not speaker_name:
        speaker_name = doc.get('celeJmeno')

    order = doc.get('poradi')
    model = {
        'cur_id': url_id,
        'title': doc.get('Id'),
        'text': doc.get('text'),
        'day': day_str,
        'speaker_name': speaker_name,
        'speaker_card': speaker_card,
        'prev_id': get_prev_speech(cur, day, order),
        'next_id': get_next_speech(cur, day, order),
        'ext_url': doc.get('url')
    }

    return model


@bp.route('/<int:url_id>')
@databased
def frame(url_id):
    doc = get_detail_doc(url_id)
    if not doc:
        abort(404)

    with g.conn.cursor() as cur:
        model = get_detail_model(cur, url_id, doc)
        return render_template('detail.html', title=doc.get('Id'), model=json.dumps(model))


@bp.route('/data/<int:url_id>')
@databased
def data(url_id):
    doc = get_detail_doc(url_id)
    if not doc:
        abort(404)

    with g.conn.cursor() as cur:
        model = get_detail_model(cur, url_id, doc)
        return jsonify(model)
