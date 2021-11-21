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
        self.link2id = {}
        cur.execute("""select url, person_id
from field
join ast_identity_card on link_id=field.id
order by url""")
        rows = cur.fetchall()
        for link, person_id in rows:
            self.link2id[link] = person_id

    def load_item(self, doc):
        day = self.extend_date(doc)
        speech_id = doc['url_id']

        speaker_id = None
        link = doc.get('speaker_url')
        if link:
            speaker_id = self.link2id.get(link)

        speech_order = doc.get('order')

        txt = doc.get('text')
        if txt:
            lst = tokenize(txt)
            length = len(lst)
        else:
            length = 0

        self.cur.execute("""insert into ast_speech(speech_id, speaker_id, speech_day, speech_order, word_count)
values(%s, %s, %s, %s, %s)
on conflict(speech_id) do update
set speaker_id=%s, speech_day=%s, speech_order=%s, word_count=%s""", (speech_id, speaker_id, day, speech_order, length, speaker_id, day, speech_order, length))


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
