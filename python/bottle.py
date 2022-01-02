#!/usr/bin/python3

# requires database filled by running condensate.py & a word list
# created by running wordlist.py

import os
import sys
from common import get_loose_path, get_option, make_connection
from morphodita_conv import make_tagger, simplify_fulltext
from person_mixin import PersonMixin
from show_case import ShowCase
from stop_util import load_stop_set
from token_util import tokenize

class Processor(ShowCase, PersonMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PersonMixin.__init__(self)
        self.tagger = make_tagger()
        self.stop_set = load_stop_set()
        self.simple_repre = get_option("simple_repre", "simple")

    def load_item(self, doc):
        day = self.extend_date(doc)
        speech_id = doc['url_id']
        speaker_id = self.get_person(doc)
        speech_order = doc.get('order')

        txt = doc.get('text')
        if txt:
            lst = tokenize(txt)
            length = len(lst)
            simple_text = simplify_fulltext(self.tagger, self.stop_set, txt)
        else:
            length = 0
            simple_text = None

        self.cur.execute("""insert into ast_speech(speech_id, speaker_id, speech_day, speech_order, word_count)
values(%s, %s, %s, %s, %s)
on conflict(speech_id) do update
set speaker_id=%s, speech_day=%s, speech_order=%s, word_count=%s""", (speech_id, speaker_id, day, speech_order, length, speaker_id, day, speech_order, length))

        if speaker_id:
            speaker_url = doc.get('speaker_url')
            if speaker_url:
                link_id = self.get_url_id(speaker_url)
                if link_id:
                    self.cur.execute("""insert into ast_person_card(person_id, link_id)
values(%s, %s)
on conflict do nothing""", (speaker_id, link_id))

        simple_path = get_loose_path(speech_id, alt_repre=self.simple_repre)
        if simple_text:
            with open(simple_path, 'w') as f:
                f.write(simple_text)

            self.cur.execute("""update ast_speech
set content=to_tsvector('ast_config', %s)
where speech_id=%s""", (simple_text, speech_id))
        else:
            if os.path.exists(simple_path):
                os.remove(simple_path)

            self.cur.execute("""update ast_speech
set content=null
where speech_id=%s""", (speech_id,))


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
