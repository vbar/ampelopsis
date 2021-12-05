#!/usr/bin/python3

import re
import sys
from baker import KERNEL, WOOD
from common import make_connection
from json_frame import JsonFrame
from html_lookup import make_card_query_urls
from url_templates import speaker_minister_tmpl, speaker_mp_tmpl

class Scanner(JsonFrame):
    def __init__(self, cur, wood_flag):
        JsonFrame.__init__(self, cur)
        self.level = WOOD if wood_flag else KERNEL
        self.total = 0
        self.unknown = set()

    def run(self):
        speaker_pattern = "^(%s|%s)" % (re.escape(speaker_mp_tmpl), speaker_minister_tmpl)
        self.cur.execute("""select url, id
from field
where url ~ '%s'
order by url""" % speaker_pattern)
        rows = self.cur.fetchall()
        for row in rows:
            self.check_card(*row)

    def dump_summary(self):
        print("%d unknown / %d cards" % (len(self.unknown), self.total))

    def dump(self):
        for name in sorted(self.unknown):
            print(name)

    def check_card(self, card_url, url_id):
        print("checking %s..." % (card_url,), file=sys.stderr)
        self.total += 1

        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print(card_url + " not downloaded", file=sys.stderr)
            return None

        try:
            qurls = make_card_query_urls(card_url, self.level, reader)
            success = 0
            for qurl in qurls:
                if self.check_query(qurl):
                    success += 1

            if not success:
                self.unknown.add(card_url)
        finally:
            reader.close()

    def check_query(self, url):
        url_id = self.get_url_id(url)
        if not url_id:
            return False

        doc = self.get_document(url_id)
        if not doc or not isinstance(doc, dict):
            return False

        results = doc.get('results')
        if not results or not isinstance(results, dict):
            return False

        bindings = results.get('bindings')
        if not bindings or not isinstance(bindings, list):
            return False

        return len(bindings) > 0


def main():
    wood_flag = False
    summary_flag = False
    for a in sys.argv[1:]:
        if a in ('-s', '--summary'):
            summary_flag = True
        elif a in ('-w', '--wood'):
            wood_flag = True
        else:
            raise Exception("unknown argument " + raw_mode)

    with make_connection() as conn:
        with conn.cursor() as cur:
            scanner = Scanner(cur, wood_flag)
            scanner.run()
            if summary_flag:
                scanner.dump_summary()
            else:
                scanner.dump()


if __name__ == "__main__":
    main()
