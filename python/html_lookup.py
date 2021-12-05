import dateparser
from lxml import etree
import re
from baker import ALL, make_personage_query_urls
from clean_util import clean_text, clean_text_node, clean_title, clean_title_node
from personage import make_personage
from url_templates import speaker_minister_rx, speaker_mp_rx

class HtmlMpLookup:
    def __init__(self):
        # MP cards don't declare encoding; callers could get it from
        # HTTP headers (cards are never from an archive), but so far
        # it's hardcoded...
        self.html_parser = etree.HTMLParser(encoding='windows-1250')
        self.born_rx = re.compile("Narozena?: (v roce ([0-9]{4})|([0-9. ]+))")

    def make_card_person(self, card_doc):
        titles = card_doc.xpath('//title/text()')
        title = clean_title(titles)
        if not title:
            return None

        captions = card_doc.xpath('//div[@class="figure"]/div[@class="figcaption"]//text()')
        caption = clean_text(captions)
        m = self.born_rx.search(caption)
        if not m:
            return None

        raw_date = m.group(1)
        year = None
        if raw_date.startswith('v roce'):
            year = int(m.group(2))
        else:
            dt = dateparser.parse(m.group(3), languages=['cs'])
            if dt:
                year = dt.year

        if not year:
            return None

        return make_personage(title, year)


class HtmlMinisterLookup:
    def __init__(self):
        # vlada.cz declares charset inside HTML
        self.html_parser = etree.HTMLParser()
        self.born_rx = re.compile("\\b[Nn]aro(?:dil|zen).+?([0-9]{4})")

    def make_card_person(self, card_doc):
        headers = card_doc.xpath('//h1/text()')
        if len(headers) != 1:
            return None

        header = clean_title_node(headers[0])
        if not header:
            return None

        paras = card_doc.xpath('//p/text()')
        for raw_text in paras:
            par_text = clean_text_node(raw_text)
            m = self.born_rx.search(par_text)
            if m:
                year = int(m.group(1))
                return make_personage(header, year)

        return None


def make_card_person(url, fp):
    inner_lookup = None
    m = speaker_mp_rx.match(url)
    if m:
        inner_lookup = HtmlMpLookup()
    else:
        m = speaker_minister_rx.match(url)
        if m:
            inner_lookup = HtmlMinisterLookup()

    if not inner_lookup:
        raise Exception("%s not a personal card" % url)

    doc = etree.parse(fp, inner_lookup.html_parser)
    return inner_lookup.make_card_person(doc)


def make_card_query_urls(url, level, fp):
    person = make_card_person(url, fp)
    return make_personage_query_urls(person, level) if person else []


def make_all_card_query_urls(url, fp):
    return make_card_query_urls(url, ALL, fp)
