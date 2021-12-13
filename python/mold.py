#!/usr/bin/python3

import json
import os
import sys
from bench_mixin import BenchMixin
from common import get_loose_path, get_option, make_connection
from cook import make_speaker_position_set, make_speaker_query_urls
from morphodita_conv import make_tagger
from speech_saw import SpeechSaw
from url_templates import session_archive_tmpl, session_page_tmpl, synth_url_tmpl
from url_util import make_url_pattern

synth_url_format = synth_url_tmpl.format('{0:04d}', '{1:03d}', '{2:06d}')

class SpeechInserter(SpeechSaw, BenchMixin):
    def __init__(self, cur, tagger, last_date=None, last_order=0):
        SpeechSaw.__init__(self, cur, tagger, last_date, last_order)
        BenchMixin.__init__(self)
        self.plain_repre = get_option("plain_repre", "plain")
        self.known = set() # of (str speaker_name, str position_list, int card URL id)

    def pass_out(self, out):
        self.ensure_speaker(out)
        self.write_document(out)

    def ensure_speaker(self, out):
        speaker_position = out.get('speaker_position')
        speaker_name = out.get('speaker_name')
        if not (speaker_position and speaker_name):
            return

        speaker_url_id = None
        speaker_url = out.get('speaker_url')
        if speaker_url:
            speaker_url_id = self.get_url_id(speaker_url)

        position_set = make_speaker_position_set(speaker_position)
        if not len(position_set):
            return

        position_list = " ".join(sorted(position_set))
        kt = (speaker_name, position_list, speaker_url_id)
        if kt in self.known:
            return

        qurls = make_speaker_query_urls(speaker_name, position_set)
        for qurl in qurls:
            self.process_query(speaker_url_id, None, position_set, qurl)

        self.known.add(kt)

    def write_document(self, out):
        synth_url = synth_url_format.format(out['legislature'], out['session'], out['order'])
        self.cur.execute("""insert into field(url, checkd, parsed)
values(%s, localtimestamp, localtimestamp)
on conflict do nothing
returning id""", (synth_url,))
        row = self.cur.fetchone()
        if row is None:
            print("%s already exists" % synth_url, file=sys.stderr)
            self.cur.execute("""select id
from field
where url=%s""", (synth_url,))
            row = self.cur.fetchone()

        url_id = row[0]
        txt = out.pop('text', None)
        with open(get_loose_path(url_id), 'w') as f:
            json.dump(out, f, ensure_ascii=False)

        plain_path = get_loose_path(url_id, alt_repre=self.plain_repre)
        if txt:
            with open(plain_path, 'w') as f:
                f.write(txt)
        else:
            if os.path.exists(plain_path):
                os.remove(plain_path)


def insert_archives(cur, tagger):
    pattern = make_url_pattern(session_archive_tmpl)
    select = """select url
from field
left join download_error on id=url_id
where (url ~ '%s') and (url_id is null)
order by url""" % pattern
    cur.execute(select)
    rows = cur.fetchall()
    for row in rows:
        archive_url = row[0]
        print("splitting %s..." % archive_url, file=sys.stderr)
        builder = SpeechInserter(cur, tagger)
        builder.run(archive_url)
        builder.flush()


def insert_standalones(cur, tagger):
    pattern = make_url_pattern(session_page_tmpl, last_grp="([0-9]+)-([0-9]+)")
    # matches[2]::integer should equal matches[3]::integer
    select = """select url, matches[1]::integer as legis, matches[2]::integer as sess
from ( select url, regexp_matches(url, '%s') as matches
        from field
        where url ~ '%s'
) as pages
order by matches[1]::integer, matches[2]::integer, matches[3]::integer, matches[4]::integer""" % (pattern, pattern)
    cur.execute(select)
    rows = cur.fetchall()
    builder = None
    for row in rows:
        url, legislature_id, session_id = row
        print("splitting %s..." % url, file=sys.stderr)
        if builder is None:
            builder = SpeechInserter(cur, tagger)
        elif (legislature_id != int(builder.legislature_id)) or (session_id != int(builder.session_id)):
            builder.flush()
            last_date = builder.current_date
            last_order = builder.processing_order
            builder = SpeechInserter(cur, tagger, last_date, last_order)

        builder.run(url)

    if builder is not None:
        builder.flush()


def main():
    print("loading tagger...", file=sys.stderr)
    tagger = make_tagger()

    conn = make_connection()
    try:
        with conn.cursor() as cur:
            insert_standalones(cur, tagger)
            # archives inserted last to take precedence
            insert_archives(cur, tagger)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
