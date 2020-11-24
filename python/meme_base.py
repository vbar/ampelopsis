import collections
from dateutil.parser import parse
import re
from pinhole_base import PinholeBase

# meme is either hashtag, or (absolute, Twitter-shortened) URL
MemeOcc = collections.namedtuple('MemeOcc', 'meme hamlet_name occ_date')

class MemeBase(PinholeBase):
    def __init__(self, cur, meme_regex, distinguish, deconstructed):
        PinholeBase.__init__(self, cur, distinguish, deconstructed)
        self.meme_rx = re.compile(meme_regex)
        self.meme_line = [] # of MemeOcc
        self.vars2memes = collections.defaultdict(set) # pair of variants -> set of meme strings

    def load_item(self, et):
        hamlet_name = et['osobaid']
        occ_date = parse(et['datum'])
        txt = et['text']
        for m in self.meme_rx.finditer(txt):
            self.meme_line.append(MemeOcc(meme=m.group(1), hamlet_name=hamlet_name, occ_date=occ_date))

    def lazy_ref_map(self):
        if len(self.ref_map):
            return

        meme_line = sorted(self.meme_line, key=lambda mo: mo.occ_date)
        if not len(meme_line):
            return

        self.mindate = meme_line[0].occ_date
        self.maxdate = meme_line[-1].occ_date

        topics = {} # meme => set of variant
        for mo in meme_line:
            variant = self.get_variant(mo.hamlet_name)
            if variant is not None:
                top = topics.get(mo.meme)
                if top is None:
                    originator = set()
                    originator.add(variant)
                    topics[mo.meme] = originator
                elif variant not in top:
                    target_node = self.introduce_node(variant, False)
                    for prev_variant in top:
                        source_node = self.introduce_node(prev_variant, self.distinguish)
                        edge = (source_node, target_node)
                        weight = self.ref_map.get(edge, 0)
                        self.ref_map[edge] = weight + 1

                        edge_memes = self.vars2memes[(prev_variant, variant)]
                        edge_memes.add(mo.meme)

                    top.add(variant)
