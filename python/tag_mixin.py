import collections

TagOcc = collections.namedtuple('TagOcc', 'tag hamlet_name occ_date')

class TagMixin:
    def __init__(self):
        self.tag_line = [] # of TagOcc
        self.vars2tags = collections.defaultdict(set) # pair of variants -> set of tag strings

    # can't define lazy_ref_map in a mixin - PinholeBase default would
    # be preferred
    def do_ref_map(self):
        if len(self.ref_map):
            return

        tag_line = sorted(self.tag_line, key=lambda tgo: tgo.occ_date)
        if not len(tag_line):
            return

        self.mindate = tag_line[0].occ_date
        self.maxdate = tag_line[-1].occ_date

        topics = {} # tag => set of variant
        for tgo in tag_line:
            variant = self.get_variant(tgo.hamlet_name)
            if variant is not None:
                top = topics.get(tgo.tag)
                if top is None:
                    originator = set()
                    originator.add(variant)
                    topics[tgo.tag] = originator
                elif variant not in top:
                    target_node = self.introduce_node(variant, False)
                    for prev_variant in top:
                        source_node = self.introduce_node(prev_variant, self.distinguish)
                        edge = (source_node, target_node)
                        weight = self.ref_map.get(edge, 0)
                        self.ref_map[edge] = weight + 1

                        edge_tags = self.vars2tags[(prev_variant, variant)]
                        edge_tags.add(tgo.tag)

                    top.add(variant)
