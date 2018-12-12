import re
from corrector import Corrector
from named_entities import rector_of_charles_university_position_entity
from rulebook_util import get_org_name

charles_university = {
    'univerzita karlova',
    'univerzita karlova v praze'
}

university_name_rx = re.compile("\\b(?:univerzita|učení|škola)")

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

# Match for an academic functionary
class UniversityLevel:
    # currently set up for just Charles University, but can be
    # extended, if there are other rector entities...
    def __init__(self):
        self.university_corrector = Corrector(4, charles_university)

    def __call__(self, it):
        org_name = get_org_name(it)
        university = self.university_corrector.match(org_name)
        found_rector = len(university)

        sought = []
        if found_rector:
            sought.append(rector_of_charles_university_position_entity)

        if found_rector or university_name_rx.search(org_name):
            sought.extend([ 'Q212071', 'Q2113250', 'Q723682' ])

        return sought

# Match for a city council, or some other council; currently we just
# check regions (and drop some obscure orgs).
class CouncilLevel(MuniLevel):
    def __init__(self, unknown_council_set, region2councillor, default_level):
        self.unknown_council_set = unknown_council_set
        self.region2councillor = region2councillor
        self.region_corrector = Corrector(4, region2councillor.keys())
        self.default_level = default_level

    def __call__(self, it):
        org_name = get_org_name(it)

        if org_name in self.unknown_council_set:
            # this match isn't for a municipality and doesn't
            # contribute to city set
            return []

        if self.region_corrector.is_correct(org_name):
            return self.region2councillor[org_name]

        region_entities = set()
        regions = self.region_corrector.match(org_name)
        for reg in regions:
            ent = self.region2councillor[reg]
            if isinstance(ent, str):
                region_entities.add(ent)
            else:
                region_entities.update(ent)

        if len(region_entities):
            return region_entities
        else:
            return self.default_level(it)
