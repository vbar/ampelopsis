import collections
from datetime import datetime
from flask import abort, Blueprint, g, jsonify, render_template, request
from .database import databased
from .shared_model import list_persons

SearchFilter = collections.namedtuple('SearchFilter', 'start_date end_date search_text')

bp = Blueprint('overview', __name__, template_folder='templates')

def list_days(cur, sf):
    description = []
    if not sf.search_text:
        cur.execute("""select speech_day, sum(word_count) total
from steno_speech
where (speech_day>=%s) and (speech_day<=%s)
group by speech_day
order by speech_day""", (sf.start_date, sf.end_date))
    else:
        cur.execute("""select speech_day, sum(word_count) total
from steno_speech
where (speech_day>=%s) and (speech_day<=%s) and (content @@ to_tsquery('steno_config', %s))
group by speech_day
order by speech_day""", (sf.start_date, sf.end_date, sf.search_text))

    rows = cur.fetchall()
    for day, total in rows:
        out = [ day.isoformat(), total ]
        description.append(out)

    return description


def list_details(cur, sf):
    daylines = []
    cur_day = None
    timeline = None
    if not sf.search_text:
        cur.execute("""select speech_day, speech_id, speaker_id, word_count
from steno_speech
where (speech_day>=%s) and (speech_day<=%s)
order by speech_day, speech_order""", (sf.start_date, sf.end_date))
    else:
        cur.execute("""select speech_day, speech_id, speaker_id, word_count
from steno_speech
where (speech_day>=%s) and (speech_day<=%s) and (content @@ to_tsquery('steno_config', %s))
order by speech_day, speech_order""", (sf.start_date, sf.end_date, sf.search_text))

    rows = cur.fetchall()
    for day, speech_id, speaker_id, length in rows:
        if cur_day != day:
            if timeline:
                daylines.append(timeline)

            cur_day = day
            timeline = []

        item = [ speaker_id, length, speech_id ]
        timeline.append(item)

    if timeline:
        daylines.append(timeline)

    return daylines


def make_date_extent(cur):
    cur.execute("""select min(speech_day) mn, max(speech_day) mx
from steno_speech""")
    row = cur.fetchone()
    if row:
        return [ d.isoformat() for d in row ]
    else:
        return None


@bp.route("/")
@bp.route('/index')
def frame():
    return render_template('index.html', title='index')


@bp.route('/data')
@databased
def data():
    search = request.args.get('s')
    start_sec = request.args.get('ts')
    end_sec = request.args.get('tu')
    if (not start_sec) or (not end_sec):
        abort(400)

    if (not start_sec.isdigit()) or (not end_sec.isdigit()):
        abort(400)

    start_date = datetime.utcfromtimestamp(int(start_sec))
    end_date = datetime.utcfromtimestamp(int(end_sec))
    search_filter = SearchFilter(start_date, end_date, search)
    with g.conn.cursor() as cur:
        names, colors = list_persons(cur)
        custom = {
            'names': names,
            'colors': colors,
            'dayDesc': list_days(cur, search_filter),
            'dayLines': list_details(cur, search_filter),
            'dateExtent': make_date_extent(cur)
        }

        return jsonify(custom)
