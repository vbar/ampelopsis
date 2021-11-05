#!/usr/bin/python3

# requires database filled by running condensate.py

import os
import sys
from common import get_loose_path, get_option, make_connection
from show_case import ShowCase
from token_util import tokenize

class Processor(ShowCase):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        self.simple_repre = get_option("simple_representation", "simple")
        self.hamlet2id = {}
        cur.execute("""select id, hamlet_name
from steno_record
order by id""")
        rows = cur.fetchall()
        for person_id, hamlet_name in rows:
            self.hamlet2id[hamlet_name] = person_id

    def load_item(self, rec):
        day = self.extend_date(rec)
        speech_id = rec['url_id']

        hamlet_name = rec['OsobaId']
        speaker_id = self.hamlet2id.get(hamlet_name)

        speech_order = rec.get('poradi')

        txt = rec.get('text')
        if txt:
            lst = tokenize(txt)
            length = len(lst)
        else:
            length = 0

        self.cur.execute("""insert into steno_speech(speech_id, speaker_id, speech_day, speech_order, word_count)
values(%s, %s, %s, %s, %s)
on conflict(speech_id) do update
set speaker_id=%s, speech_day=%s, speech_order=%s, word_count=%s""", (speech_id, speaker_id, day, speech_order, length, speaker_id, day, speech_order, length))

        simple_path = get_loose_path(speech_id, alt_repre=self.simple_repre)
        if os.path.exists(simple_path):
            with open(simple_path, 'r') as f:
                content = f.read()
                self.cur.execute("""update steno_speech
set content=to_tsvector('steno_config', %s)
where speech_id=%s""", (content, speech_id))


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            processor = Processor(cur)
            processor.run()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
