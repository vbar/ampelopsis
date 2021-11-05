import collections
from datetime import datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from flask import abort, g, jsonify, render_template, request
from .database import databased
from .filesystem import get_detail_doc
from steno import app

Speaker = collections.namedtuple('Speaker', 'name card')

SearchFilter = collections.namedtuple('SearchFilter', 'start_date end_date search_text')

def list_persons(cur):
    names = []
    colors = []
    cur.execute("""select steno_record.id, presentation_name, color
from steno_record
left join steno_party on steno_party.id=steno_record.party_id
order by id""")
    rows = cur.fetchall()
    for person_id, person_name, party_color in rows:
        while len(names) < person_id:
            names.append("")

        while len(colors) < person_id:
            colors.append("")

        names.append(person_name)

        bare_color = party_color or 'AAA'
        colors.append('#' + bare_color)

    return (names, colors)


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


@app.route("/")
@app.route('/index')
def index():
    return render_template('index.html', title='index')


@app.route('/daytime')
@databased
def daytime():
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


@app.route('/detail/<int:url_id>')
@databased
def url(url_id):
    doc = get_detail_doc(url_id)
    if not doc:
        abort(404)

    with g.conn.cursor() as cur:
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
            'text': doc.get('text'),
            'day': day_str,
            'speaker_name': speaker_name,
            'speaker_card': speaker_card,
            'prev_id': get_prev_speech(cur, day, order),
            'next_id': get_next_speech(cur, day, order),
            'ext_url': doc.get('url')
        }

        return render_template('detail.html', title=doc.get('Id'), model=model)
