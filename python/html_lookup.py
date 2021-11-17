import dateparser
from lxml import etree
import re
from baker import make_personage_query_urls
from clean_util import clean_text
from personage import make_personage

born_rx = re.compile("Narozena?: (v roce ([0-9]{4})|([0-9. ]+))")

class HtmlLookup:
    def __init__(self):
        self.encoding2parser = {} # str encoging -> HTML parser

    # MP cards don't declare encoding; callers could get it from HTTP
    # headers (cards are never from an archive), but so far it's
    # hardcoded...
    def ensure_html_parser(self, encoding):
        html_parser = self.encoding2parser.get(encoding)
        if html_parser is None:
            enc_arg = encoding if encoding else None
            html_parser = etree.HTMLParser(encoding=enc_arg)
            self.encoding2parser[encoding] = html_parser

        return html_parser

    def make_mp_query_urls(self, card_doc):
        titles = card_doc.xpath('//title/text()')
        title = clean_text(titles)
        if not title:
            return []

        captions = card_doc.xpath('//div[@class="figure"]/div[@class="figcaption"]//text()')
        caption = clean_text(captions)
        m = born_rx.search(caption)
        if not m:
            return []

        raw_date = m.group(1)
        year = None
        if raw_date.startswith('v roce'):
            year = int(m.group(2))
        else:
            dt = dateparser.parse(m.group(3), languages=['cs'])
            if dt:
                year = dt.year

        if not year:
            return []

        person = make_personage(title, year)
        return make_personage_query_urls(person)
