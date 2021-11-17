#!/usr/bin/python3

import dateparser
from lxml import etree
import re
import sys
from urllib.parse import urljoin, urlparse, urlunparse
import yaml
import zipfile
from common import make_connection
from cursor_wrapper import CursorWrapper
from morphodita_conv import make_tagger, split_position_name
from url_templates import page_local_rx, segment_local_rx, segment_rx, session_archive_rx, session_folder_tmpl, session_page_rx, speaker_rx
from volume_holder import VolumeHolder

nbsp_rx = re.compile('(?:\xa0|\\s)+')

def clean_text_node(raw_text):
    text = nbsp_rx.sub(' ', raw_text)
    return text.strip()


def clean_text(text_nodes):
    texts = []
    for raw_text in text_nodes:
        texts.append(clean_text_node(raw_text))

    return " ".join(texts)


class SpeechSaw(VolumeHolder, CursorWrapper):
    def __init__(self, cur, tagger):
        VolumeHolder.__init__(self)
        CursorWrapper.__init__(self, cur)
        self.html_parser = etree.HTMLParser()
        self.tagger = tagger
        self.orig_url = None
        self.legislature_id = None
        self.session_id = None
        self.base = None
        self.nav_count = None
        self.current_date = None
        self.current_speaker = None
        self.current_text = None
        self.processing_order = 0

    def run(self, url):
        assert url

        schema = (
            ( session_archive_rx, self.process_archive ),
            ( session_page_rx, self.process_standalone_day ),
            ( segment_rx, self.process_standalone_segment )
        )

        m = None
        process = None
        for url_rx, proc_meth in schema:
            m = url_rx.match(url)
            if m:
                process = proc_meth
                break

        if not m:
            print(url + " has unknown format", file=sys.stderr)
            return

        self.legislature_id = m.group(1)
        self.session_id = m.group(2)
        self.do_run(url, process)

    def do_run(self, url, process):
        self.cur.execute("""select id, checkd
from field
where url=%s""", (url,))
        row = self.cur.fetchone()
        if not row:
            print("unknown URL " + url, file=sys.stderr)
            return

        if row[1] is None:
            print(url + " not downloaded", file=sys.stderr)
            return

        url_id = row[0]
        volume_id = self.get_volume_id(url_id)
        fp = self.open_page(url_id, volume_id)
        if not fp:
            print(url + " has no body", file=sys.stderr)
            return

        try:
            self.orig_url = url
            process(fp)
        finally:
            fp.close()

    def process_standalone_segment(self, fp):
        self.base = self.orig_url
        self.handle_segment(fp)

    def process_archive(self, fp):
        base_url = session_folder_tmpl.format(self.legislature_id, self.session_id)
        with zipfile.ZipFile(fp) as zp:
            day_map = {} # (int, int) -> info
            segment_map = {} # filename -> info
            for info in zp.infolist():
                filename = info.filename
                m = page_local_rx.match(filename)
                if m:
                    key = tuple([ int(m.group(n)) for n in range(1, 3) ])
                    day_map[key] = info
                elif segment_local_rx.match(filename):
                    segment_map[filename] = info

            for key, info in sorted(day_map.items()):
                self.base = base_url + info.filename
                with zp.open(info) as f:
                    self.process_archive_day(zp, segment_map, f)

    def process_archive_day(self, zp, segment_map, fp):
        base_url = session_folder_tmpl.format(self.legislature_id, self.session_id)
        doc = etree.parse(fp, self.html_parser)
        self.check_base(doc)
        self.update_day_date(doc)

        anchors = doc.xpath('.//a')
        segments = []
        for a in anchors:
            href = a.get('href')
            if href:
                frag = href.find('#')
                local_name = href[:frag] if frag >= 0 else href
                if local_name in segment_map:
                    if not len(segments) or (segments[-1] != local_name):
                        segments.append(local_name)

        for local_name in segments:
            info = segment_map[local_name]
            with zp.open(info) as f:
                self.base = base_url + local_name
                self.handle_segment(f)

    def process_standalone_day(self, fp):
        self.base = self.orig_url
        doc = etree.parse(fp, self.html_parser)
        self.check_base(doc)
        self.update_day_date(doc)

        anchors = doc.xpath('.//a')
        segments = []
        for a in anchors:
            href = a.get('href')
            if href:
                link = urljoin(self.base, href)
                if segment_rx.match(link):
                    pr = urlparse(link)
                    defrag_pr = (pr.scheme, pr.hostname, pr.path, pr.params, pr.query, '')
                    segment_url = urlunparse(defrag_pr)
                    if not len(segments) or (segments[-1] != segment_url):
                        segments.append(segment_url)

        # not in cycle above b/c it updates self.base
        for segment_url in segments:
            self.do_run(segment_url, self.process_standalone_segment)

    def handle_segment(self, fp):
        self.nav_count = 0
        doc = etree.parse(fp, self.html_parser)
        self.check_base(doc)

        paras = doc.xpath("//p")
        for p in paras:
            if not self.accumulate(p):
                return

    def check_base(self, doc):
        # parser might handle HTML base, but mostly because it's
        # based on a generic one - it isn't expected for this
        # project...
        bases = doc.xpath("//base")
        if len(bases):
            raise Exception("FIXME: process HTML base")

    def update_day_date(self, doc):
        titles = doc.xpath("/html/head/title")
        if len(titles) == 1:
            title = titles[0]
            txt = title.text
            if txt:
                sgmt = txt.split(",")
                if len(sgmt) == 2:
                    dt = dateparser.parse(sgmt[1].strip(), languages=['cs'])
                    if dt:
                        self.current_date = dt

    def accumulate(self, p):
        pid = p.get('id')
        if pid in ('breadcrumb', 'logo'):
            return True

        pclass = p.get('class')
        if pclass == 'no-screen':
            return True
        elif pclass == 'date':
            date_text = clean_text(p.xpath('.//text()'))
            if date_text:
                dt = dateparser.parse(date_text, languages=['cs'])
                if dt:
                    self.current_date = dt

            return True

        anchors = p.xpath('.//a')
        switched = False
        par_nav_count = 0
        for a in anchors:
            href = a.get('href')
            if href:
                link = urljoin(self.base, href)
                if speaker_rx.match(link):
                    if switched:
                        raise Exception("multiple speakers in one paragraph")

                    self.flush()

                    self.current_speaker = {
                        'url': link
                    }

                    speaker_text = clean_text(a.xpath('.//text()'))
                    if speaker_text:
                        self.current_speaker['text'] = speaker_text
                        pn = split_position_name(self.tagger, speaker_text, True)
                        if pn:
                            self.current_speaker['position'] = pn[0]
                            self.current_speaker['name'] = pn[1]

                    self.current_text = ""
                    switched = True
                # fragments of this page would match segment; consider
                # only links to different pages
                elif (href[0] != '#') and segment_rx.match(link):
                    par_nav_count += 1

        if par_nav_count:
            self.nav_count += 1
            return self.nav_count == 1

        text_nodes = p.xpath('.//text()')
        if len(text_nodes):
            speaker_text = clean_text_node(text_nodes[0])
            if switched: # no reason to parse the name twice...
                # ...but we should check
                if self.current_speaker['text'] != speaker_text:
                    raise Exception("anchor text doesn't match paragraph start")

                text_nodes = text_nodes[1:]
            else:
                pn = split_position_name(self.tagger, speaker_text, False)
                if pn:
                    self.flush()

                    self.current_speaker = {
                        'text': speaker_text,
                        'position': pn[0],
                        'name': pn[1]
                    }

                    self.current_text = ""
                    # not setting switched b/c it isn't used below
                    text_nodes = text_nodes[1:]

        if not self.current_speaker: # didn't start yet
            return True

        text = clean_text(text_nodes)
        if text:
            if self.current_text:
                self.current_text += "\n"

            self.current_text += text

        return True

    def flush(self):
        if not self.current_speaker:
            return

        self.processing_order += 1

        if self.current_text and (self.current_text[0] == ":"):
            current_text = self.current_text[1:].lstrip()
        else:
            current_text = self.current_text

        out = {
            'legislature': int(self.legislature_id),
            'session': int(self.session_id),
            'order': self.processing_order,
            'text': current_text,
            'orig_url': self.orig_url
        }

        for field_name in ('text', 'position', 'name', 'url'):
            if field_name in self.current_speaker:
                out['speaker_' + field_name] = self.current_speaker[field_name]

        if self.current_date:
            out['date'] = self.current_date.isoformat()

        self.pass_out(out)


class SpeechPrinter(SpeechSaw):
    def pass_out(self, out):
        print(yaml.dump(out, allow_unicode=True))


def main():
    conn = make_connection()
    try:
        with conn.cursor() as cur:
            print("loading tagger...", file=sys.stderr)
            tagger = make_tagger()
            saw = SpeechPrinter(cur, tagger)
            try:
                for a in sys.argv[1:]:
                    saw.run(a)

                saw.flush()
            finally:
                saw.close()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
