#!/usr/bin/python3

import re
import sys
from baker import make_personage_query_urls, WOOD
from bench_mixin import BenchMixin
from common import make_connection
from cook import make_speaker_query_urls, minister_position
from html_lookup import make_card_person
from json_frame import JsonFrame
from url_templates import speaker_minister_rx, speaker_minister_tmpl, speaker_mp_tmpl

class Condensator(JsonFrame, BenchMixin):
    def __init__(self, cur):
        JsonFrame.__init__(self, cur)
        BenchMixin.__init__(self)

    def run(self):
        speaker_pattern = "^(%s|%s)" % (re.escape(speaker_mp_tmpl), speaker_minister_tmpl)
        self.cur.execute("""select url, id
from field
left join download_error on id=url_id
where url ~ '%s'
and checkd is not null
and url_id is null
order by url""" % speaker_pattern)
        rows = self.cur.fetchall()
        for row in rows:
            self.process_card(*row)

    def process_card(self, card_url, url_id):
        print("checking %s..." % (card_url,), file=sys.stderr)

        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            # happens e.g. for (http -> https) redirects
            print("%s not downloaded" % (card_url,), file=sys.stderr)
            return

        try:
            person = make_card_person(card_url, reader)
            if person:
                birth_year = person.birth_year
                position_set = None
                qurls = []
                if birth_year:
                    qurls.extend(make_personage_query_urls(person, WOOD))

                if speaker_minister_rx.match(card_url):
                    position_set = set((minister_position,))
                    qurls.extend(make_speaker_query_urls(person.presentation_name, position_set))

                for qurl in qurls:
                    self.process_query(url_id, birth_year, position_set, qurl)
        finally:
            reader.close()


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            condensator = Condensator(cur)
            try:
                condensator.run()
            finally:
                condensator.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
