import re
from corrector import Corrector
from named_entities import Entity, region_councillor_entities
from rulebook_util import convert_answer_to_iterable, get_org_name, university_name_rx

charles_university = {
    'univerzita karlova',
    'univerzita karlova v praze'
}

hospital_name_rx = re.compile("^nemocnice\\b")

# A rulebook (q.v.) value marking the match as an MP position that
# should also have its terms checked. Note that despite the name,
# instances of this object can only be initialized with Entity.mp -
# adding senators' terms would require changes in Jumper.
class ParliamentLevel:
    def __init__(self, position):
        self.position = position

    def __call__(self, it):
        return self.position

# Match for a judge. Not configurable - differs from simply using
# Entity.judge by allowing Jumper to recognize the class
# name. Admittedly Jumper could also recognize Entity.judge, but just
# to keep the level processing somewhat regular...
class JudgeLevel:
    def __call__(self, it):
        return Entity.judge

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

class OrgLevel:
    def __init__(self, organization2occupation):
        self.organization2occupation = organization2occupation

    # not actually used, since this class is only used as base, and
    # all derived classes use match directly
    def __call__(self, it):
        org_name = get_org_name(it)
        return self.match(org_name)

    def match(self, org_name):
        for org, occ in self.organization2occupation.items():
            if org_name.startswith(org):
                return occ

        return None

# Match for an academic functionary.
class UniversityLevel(OrgLevel):
    # currently set up for just Charles University, but can be
    # extended, if there are other rector entities...
    def __init__(self, organization2occupation, upper):
        OrgLevel.__init__(self, organization2occupation)
        self.upper = upper
        self.university_corrector = Corrector(4, charles_university)

    def __call__(self, it):
        org_name = get_org_name(it)
        base_found = self.match(org_name)
        if base_found:
            return base_found

        university = self.university_corrector.match(org_name)
        found_uni = len(university)

        sought = []
        if found_uni and self.upper:
            sought.append(Entity.rector_of_charles_university)

        if found_uni or university_name_rx.search(org_name):
            # Entity.pedagogue could be added here, but it causes
            # false matches on upper level (although they might be
            # worth it?) and doesn't match anybody new on the lower
            # one...
            if self.upper:
                sought.extend(('Q212071', 'Q2113250', 'Q723682'))
            else:
                # Q43845 "professor" occupation could also be added
                # here, but it doesn't match anybody new...
                sought.append(Entity.academic)
                sought.append(Entity.researcher)
                sought.append(Entity.university_teacher)

        return sought

class PoliceLevel:
    def __init__(self, top):
        # police director is not the same as police officer, but implies it
        self.entities = [ Entity.director, Entity.police_officer ]
        if top:
            self.entities.append(Entity.president)

    def __call__(self, it):
        return self.entities

class DirectorLevel(OrgLevel):
    def __init__(self, organization2occupation):
        OrgLevel.__init__(self, organization2occupation)
        self.org_corrector = Corrector(2, organization2occupation.keys())

    def __call__(self, it):
        org_name = get_org_name(it)
        base_found = self.match(org_name)
        if base_found:
            return base_found

        entities = set()
        if hospital_name_rx.match(org_name):
            entities.add(Entity.physician)

        orgs = self.org_corrector.match(org_name)
        for org in orgs:
            ent = self.organization2occupation[org]
            if isinstance(ent, str):
                entities.add(ent)
            else:
                entities.update(ent)

        if not len(entities):
            entities.add(Entity.director)

        return entities

# values of region2councillor map, for the keys (regions) for which a
# specific entity exists; see CouncilLevel
class RegionCouncilLevel:
    def __init__(self, local_entity):
        assert local_entity
        self.local_entity = local_entity

    def __call__(self, dummy):
        entities = set(region_councillor_entities)
        entities.add(self.local_entity)
        return entities

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
            return convert_answer_to_iterable(self.region2councillor[org_name], it)

        region_entities = set()
        regions = self.region_corrector.match(org_name)
        for reg in regions:
            ent = self.region2councillor[reg]
            entities = convert_answer_to_iterable(ent, it)
            region_entities.update(entities)

        if len(region_entities):
            return region_entities
        else:
            return self.default_level(it)
