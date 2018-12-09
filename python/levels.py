from rulebook_util import get_org_name

# A rulebook (q.v.) value marking the match as an MP position that
# should also have its terms checked. Note that despite the name,
# instances of this object can only be initialized with
# mp_position_entity - adding senators' terms would require changes in
# Jumper.
class ParliamentLevel:
    def __init__(self, position):
        self.position = position

    def __call__(self, it):
        return self.position

# A rulebook (q.v.) value marking the match as a position that should
# also match a city/village. Note that instances of this object can be
# values of multiple rulebook keys, but each individual instance must
# be initialized with a non-overlapping position set (technically an
# iterable, or a single string).
class MuniLevel:
    def __init__(self, positions):
        self.positions = positions

    def __call__(self, it):
        return self.positions

# Match for a city council, or some other council; currently we just
# check regions (and drop some obscure orgs).
class CouncilLevel(MuniLevel):
    def __init__(self, unknown_council_set, region2councillor, default_level):
        self.unknown_council_set = unknown_council_set
        self.region2councillor = region2councillor
        self.default_level = default_level

    def __call__(self, it):
        org_name = get_org_name(it)

        if org_name in self.unknown_council_set:
            # this match isn't for a municipality and doesn't
            # contribute to city set
            return []

        pos = self.region2councillor.get(org_name)
        if pos:
            # ditto
            return pos
        else:
            return self.default_level(it)
