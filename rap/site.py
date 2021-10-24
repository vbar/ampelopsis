from flask import abort, g, jsonify, render_template, request, send_file, url_for
from .database import databased
from .filesystem import get_detail_path
from rap import app

def list_submitters(cur):
    submitters = []
    cur.execute("""select id, submitter
from rap_submitters
order by id""")
    rows = cur.fetchall()
    for submitter_id, submitter_name in rows:
        while len(submitters) < submitter_id:
            submitters.append("")

        submitters.append(submitter_name)

    return submitters


def get_activity(cur, search):
    if not search:
        cur.execute("""select rap_documents.doc_id, doc_day, grouped.wc, submitter_id, doc_name
from rap_documents
join (
        select doc_id, sum(word_count) wc
        from rap_attachments group by doc_id
) grouped on grouped.doc_id=rap_documents.doc_id
where wc>0""") # log scale cannot show zero size
    else:
        cur.execute("""select rap_documents.doc_id, doc_day, grouped.wc, submitter_id, doc_name
from rap_documents
join (
        select doc_id, sum(word_count) wc
        from rap_attachments
        group by doc_id
) grouped on grouped.doc_id=rap_documents.doc_id
join (
        select doc_id, count(att_id) cnt
        from rap_attachments
        where content @@ to_tsquery('rap_config', %s)
        group by doc_id
) found on found.doc_id=rap_documents.doc_id
where wc>0 and cnt>0""", (search,))

    rows = cur.fetchall()
    data = []
    for row in rows:
        item = [ url_for('document', url_id=row[0]), row[1].isoformat() ]
        for i in range(2, 5):
            item.append(row[i])

        data.append(item)

    return data


def make_date_extent(cur):
    cur.execute("""select min(doc_day) mn, max(doc_day) mx
from rap_documents""")
    row = cur.fetchone()
    if not row:
        return None

    date_extent = []
    for cell in row:
        date_extent.append(cell.isoformat())

    return date_extent


def model_document(cur, url_id):
    cur.execute("""select pid, doc_name, doc_desc
from rap_documents
where doc_id=%s""", (url_id,))
    row = cur.fetchone()
    if not row:
        return None

    model = {
        'pid': row[0],
        'name': row[1],
        'description': row[2]
    }

    attachments = []
    cur.execute("""select att_id, att_day, att_type
from rap_attachments
where doc_id=%s
order by att_no""", (url_id,))
    for row in cur.fetchall():
        url_id = row[0]
        item = {
            'day': row[1],
            'type': row[2]
        }

        if get_detail_path(url_id):
            item['id'] = url_id

        attachments.append(item)

    model['attachments'] = attachments

    return model


@app.route("/")
@app.route('/index')
def index():
    search = request.args.get('s')
    return render_template('index.html', enable_search=True, search=search)


@app.route('/activity')
@databased
def activity():
    search = request.args.get('s')
    with g.conn.cursor() as cur:
        custom = {
            'submitters': list_submitters(cur),
            'data': get_activity(cur, search)
        }

        date_extent = make_date_extent(cur)
        if date_extent:
            custom['dateExtent'] = date_extent

        return jsonify(custom)


@app.route('/doc/<int:url_id>')
@databased
def document(url_id):
    with g.conn.cursor() as cur:
        model = model_document(cur, url_id)
        if not model:
            abort(404)

        return render_template('document.html', title=model['pid'], model=model)


@app.route('/detail/<int:url_id>')
def url(url_id):
    path = get_detail_path(url_id)
    if not path:
        abort(404)

    return send_file(path, mimetype='text/plain')
