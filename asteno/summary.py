from flask import Blueprint, g, jsonify, render_template
import os
import sys
from .database import databased

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from palette_factory import create_palette
from palette_lookup import get_membership

bp = Blueprint('summary', __name__, url_prefix='/summary')

class PartyDesc:
    def __init__(self, color, name):
        self.color = color
        self.name = name

    def cond_rename(self, new_name):
        if (self.name is None) or ((new_name is not None) and (len(self.name) > len(new_name))):
            self.name = new_name


def make_legend(cur):
    legend = {} # int party ID -> PartyDesc

    cur.execute("""select ast_party.id, color, party_name
from ast_party
left join ast_party_name on ast_party.id=party_id
order by ast_party.id, party_name""")
    rows = cur.fetchall()
    for party_id, color, party_name in rows:
        party_desc = legend.get(party_id)
        if party_desc is None:
            legend[party_id] = PartyDesc(color, party_name)
        else:
            party_desc.cond_rename(party_name)

    return legend


def make_summary(cur):
    summary = {} # int year -> int party ID -> int count

    palette = create_palette(cur, "ast_party.id")
    cur.execute("""select speaker_id, speech_day, word_count
from ast_speech
order by speech_day, speech_order""")
    rows = cur.fetchall()
    for speaker_id, speech_day, word_count in rows:
        party_id = get_membership(palette, speaker_id, speech_day)
        if party_id:
            party2count = summary.setdefault(speech_day.year, {})
            cnt = party2count.get(party_id, 0)
            party2count[party_id] = cnt + word_count

    return summary


def format_data(legend, summary):
    out_summary = []
    out_legend = {}
    party2total = {}
    for year, party2count in sorted(summary.items()):
        item = {
            'year': year
        }

        for party_id, cnt in sorted(party2count.items()):
            old_cnt = party2total.get(party_id, 0)
            party2total[party_id] = old_cnt + cnt
            sid = str(party_id)
            item[sid] = cnt
            if sid not in out_legend:
                party_desc = legend.get(party_id)
                if party_desc:
                    name = party_desc.name if party_desc.name else ''
                    color = '#' + party_desc.color if party_desc.color else ''
                    out_legend[sid] = [ name, color ]

        out_summary.append(item)

    totals = sorted(party2total.items(), key = lambda p: (-1 * p[1], p[0]))
    return {
        'summary': out_summary,
        'legend': out_legend,
        'order': [ str(p[0]) for p in totals ]
    }


@bp.route('/')
@bp.route('/index')
def frame():
    return render_template('summary.html', title='shrnutí')


@bp.route('/data')
@databased
def data():
    with g.conn.cursor() as cur:
        legend = make_legend(cur)
        summary = make_summary(cur)
        data = format_data(legend, summary)
        return jsonify(data)
