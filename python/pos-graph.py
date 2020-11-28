#!/usr/bin/python3

# requires database filled by running condensate.py and downloaded
# data extended by running morphodita-stemmer.py

import collections
import json
import random
import re
import sys
from common import get_option, make_connection
from lang_wrap import init_lang_recog
from morphodita_tap import MorphoditaTap
from party_mixin import PartyMixin
from show_case import ShowCase
from token_util import tokenize
from url_heads import town_url_head

SentenceExample = collections.namedtuple('SentenceExample', 'text tag_group')

SentenceCacheItem = collections.namedtuple('SentenceCacheItem', 'url_set stem_set tag_set')

SentenceSample = collections.namedtuple('SentenceSample', 'text url stems tags indices')

def make_tag_filter(tag_head, raw_positions):
    lth = len(tag_head)
    position_set = set((int(p) - 1 for p in raw_positions.split()))
    if not len(position_set):
        raise Exception("no varying tag positions defined")

    positions = list(position_set)
    positions.sort(reverse=True)
    if positions[0] > 14:
        raise Exception("tag position %d too high" % positions[0])

    pos = positions.pop()
    if pos < lth:
        raise Exception("starting tag position %d too low" % pos)

    pattern = tag_head
    counter = lth
    gaps = []
    while pos is not None:
        if pos > counter:
            pattern += '.' * (pos - counter)
            counter = pos
            gaps.append(True)
        else:
            gaps.append(False)

        pattern += '(.)'
        counter += 1
        pos = positions.pop() if len(positions) else None

    return (pattern, gaps)


class Payload:
    def __init__(self, sentence):
        self.freq = 1
        self.examples = [ sentence ]

    def append(self, sentence):
        self.freq += 1
        self.examples.append(sentence)


class Processor(ShowCase, PartyMixin):
    def __init__(self, cur):
        ShowCase.__init__(self, cur)
        PartyMixin.__init__(self)
        self.restrict_persons()
        random.seed()
        self.pos_sample_max = int(get_option("pos_sample_max", "3"))
        self.lang_recog = init_lang_recog()

        self.tag_head = get_option("morphodita_tag_head", "")
        tag_positions = get_option("morphodita_tag_positions", "1") # 1-based positions
        pattern, gaps = make_tag_filter(self.tag_head, tag_positions)
        self.tag_filter_rx = re.compile(pattern)
        self.tag_group_separators = tuple([ " " if g else "" for g in gaps ])

        # tap is always unfiltered (we want whole stem sequences)
        self.tap = MorphoditaTap(cur)

        self.characteristics = {} # str hamlet name -> str MorphoDiTa tag match group -> Payload
        self.sentence_cache = {} # str sentence text -> SentenceCacheItem

    def dump(self):
        bulk = {}
        for hamlet_name, pos2payload in self.characteristics.items():
            bulk[hamlet_name] = sum((p.freq for p in pos2payload.values()))

        pos_set = set()
        data = []
        sentences = []
        text2id = {}
        for hamlet_name, pos2payload in sorted(self.characteristics.items(), key=lambda p: (-1 * bulk[p[0]], p[0])):
            pos_set.update(pos2payload.keys())
            present_name = self.person_map[hamlet_name]
            party_id = self.hamlet2party.get(hamlet_name, 0)

            pos2freq = {}
            pos2samples = {}
            for pos, payload in pos2payload.items():
                pos2freq[pos] = payload.freq

                samples = []
                for example in payload.examples:
                    sci = self.sentence_cache.get(example.text)
                    if sci:
                        if len(sci.url_set) == 1:
                            if len(sci.stem_set) == 1:
                                if len(sci.tag_set) == 1:
                                    samples.append(self.make_sentence_sample(example, sci))
                                else:
                                    print("tags vary", file=sys.stderr)
                            else:
                                print("stemming inconsistent", file=sys.stderr)
                    else:
                        raise Exception("text not cached")

                random.shuffle(samples)
                out_samples = []
                sample_idx = 0
                selected = set()
                while (sample_idx < len(samples)) and (len(out_samples) < self.pos_sample_max):
                    sample = samples[sample_idx]
                    if sample.text not in selected:
                        selected.add(sample.text)
                        txt_idx = text2id.get(sample.text)
                        if txt_idx is None:
                            txt_idx = len(sentences)
                            sentences.append({
                                'url': sample.url,
                                'text': sample.text,
                                'stems': list(sample.stems)
                            })

                            text2id[sample.text] = txt_idx

                        tags = []
                        for i in sample.indices:
                            tags.append(sample.tags[i])

                        triple = (txt_idx, sample.indices, tags)
                        out_samples.append(triple)

                    sample_idx += 1

                if len(out_samples):
                    pos2samples[pos] = out_samples

            item = {
                'name': present_name,
                'color': self.convert_color(party_id),
                'freq': pos2freq,
                'samples': pos2samples
            }

            town_name = self.hamlet2town.get(hamlet_name)
            if town_name:
                item['ext_url'] = "%s/%s" % (town_url_head, town_name)

            data.append(item)

        custom = {
            'pos': sorted(pos_set),
            'sentences': sentences,
            'data': data,
            'dateExtent': self.make_date_extent()
        }

        print(json.dumps(custom, indent=2, ensure_ascii=False))

    def load_item(self, et):
        url = et['url']
        if self.is_redirected(url):
            return

        hamlet_name = et['osobaid']
        if hamlet_name not in self.person_map:
            return

        lst = tokenize(et['text'], False)
        lng = self.lang_recog.check(lst)
        if lng != 'cs':
            return

        self.extend_date(et)
        pos2payload = self.characteristics.setdefault(hamlet_name, {})
        for sentence in self.tap.remodel(url):
            for tag in sentence.tags:
                m = self.is_matching(tag)
                if m:
                    sci = self.sentence_cache.get(sentence.text)
                    stems_tuple = tuple(sentence.stems)
                    tags_tuple = tuple(sentence.tags)
                    if sci is None:
                        self.sentence_cache[sentence.text] = SentenceCacheItem(url_set=set((url,)), stem_set=set((stems_tuple,)), tag_set=set((tags_tuple,)))
                    else:
                        sci.url_set.add(url)
                        sci.stem_set.add(stems_tuple)
                        sci.tag_set.add(tags_tuple)

                    group = self.concat_groups(m)
                    example = SentenceExample(text=sentence.text, tag_group=group)
                    payload = pos2payload.get(group)
                    if payload is None:
                        pos2payload[group] = Payload(example)
                    else:
                        payload.append(example)

    def is_matching(self, tag, grp=None):
        m = self.tag_filter_rx.match(tag)
        if not m:
            return False

        if not grp:
            return m

        sg = self.concat_groups(m)
        return sg == grp

    def concat_groups(self, m):
        group = self.tag_head
        i = 0
        for g in m.groups():
            group += self.tag_group_separators[i]
            i += 1
            group += g

        return group

    def make_sentence_sample(self, example, regular_item):
        for url in regular_item.url_set:
            for stems in regular_item.stem_set:
                for tags in regular_item.tag_set:
                    assert len(stems) == len(tags)
                    indices = []
                    for i in range(len(tags)):
                        if self.is_matching(tags[i], example.tag_group):
                            indices.append(i)

                    return SentenceSample(text=example.text, url=url, stems=stems, tags=tags, indices=indices)

    def make_date_extent(self):
        # old D3 in frontend doesn't parse ISO format...
        return [dt.strftime("%Y-%m-%d") for dt in (self.mindate, self.maxdate)]


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            processor = Processor(cur)
            try:
                processor.run()
                processor.dump()
            finally:
                processor.close()


if __name__ == "__main__":
    main()
