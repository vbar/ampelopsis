import re

from levels import CouncilLevel, DirectorLevel, JudgeLevel, MuniLevel, ParliamentLevel, RegionCouncilLevel, UniversityLevel
from named_entities import Entity, councillor_position_entities, deputy_mayor_position_entities, mayor_position_entities, region_councillor_entities
from rulebook_util import get_org_name

# Specific regional representative entities match better than the
# generic region_councillor_entities (which are also added inside
# RegionCouncilLevel), but mostly they aren't available. Prague is not
# included because it is a city, and is handled on a higher level (not
# using even Q27830328 - perhaps it should?)
region2councillor = {
    'jihočeský kraj': RegionCouncilLevel('Q55670007'),
    'jihomoravský kraj': RegionCouncilLevel('Q59583668'),
    'karlovarský kraj': region_councillor_entities,
    'kraj vysočina': region_councillor_entities,
    'krajský úřad středočeského kraje': region_councillor_entities,
    'královéhradecký kraj': RegionCouncilLevel('Q59539134'),
    'liberecký kraj': region_councillor_entities,
    'moravskoslezský kraj': RegionCouncilLevel('Q55973189'),
    'olomoucký kraj': region_councillor_entities,
    'pardubický kraj': region_councillor_entities,
    'plzeňský kraj': region_councillor_entities,
    'středočeský kraj': region_councillor_entities,
    'ústecký kraj': region_councillor_entities,
    'zlínský kraj': region_councillor_entities,
}

unknown_council_set = set([
    'dobrovolný svazek obcí moravskotřebovský vodovod',

    # the institution exists in wikidata (Q12048468) but apparently
    # not its members (see e.g. Q12049410)
    'rada pro rozhlasové a televizní vysílání',
])

# can actually contain both positions and occupations; every new
# occupation must be special-cased in Jumper
organization2occupation = {
    'úřad vlády': 'Q15712674',
    # 'kancelář prezidenta republiky' should have something as well...
    'psychiatrická nemocnice bohnice': Entity.psychiatrist,
}

# Maps it['workingPosition']['name'], where it is an item of cro
# detail page JSON attribute 'workingPositions', to position set. The
# mapping can be a single string, an iterable, or a callable returning
# a string or an iterable. The callable is called with the it value.
class Rulebook:
    def __init__(self):
        council_level = CouncilLevel(unknown_council_set, region2councillor, MuniLevel(councillor_position_entities))

        director_level = DirectorLevel(organization2occupation)

        # police director is not the same as police officer, but implies it
        police_entities = ( Entity.director, Entity.police_officer )

        self.rulebook = {
            # "governing body member" doesn't sound very university-specific,
            # but is actually used either for universities, or regional
            # councils/obscure orgs not found in Wikidata
            'člen řídícího orgánu': UniversityLevel(True),

            'vedoucí zaměstnanec 2. stupně řízení': UniversityLevel(False),
            'vedoucí zaměstnanec 3. stupně řízení': director_level,
            'vedoucí zaměstnanec 4. stupně řízení': director_level,

            # apparently doesn't include deputy ministers (but does include
            # premier)
            'člen vlády': Entity.minister,

            'náměstek člena vlády': Entity.deputy_minister,
            'náměstek pro řízení sekce': Entity.deputy_minister,
            'poslanec': ParliamentLevel(Entity.mp),
            'senátor': 'Q18941264',
            'starosta': MuniLevel(mayor_position_entities),
            'místostarosta / zástupce starosty': MuniLevel(deputy_mayor_position_entities),
            'člen zastupitelstva': council_level,
            'člen rady': council_level,

            # not clear whether input distinguishes member from governor, so
            # we don't - there shouldn't be so many of them anyway...
            'člen bankovní rady české národní banky': ( 'Q28598459', 'Q25505764' ),

            'soudce': JudgeLevel(),
            'státní zástupce': Entity.prosecutor,
            'ředitel bezpečnostního sboru': police_entities,
            # ředitel odboru/sekce doesn't match any more directors

            # 2nd level exists but doesn't match anybody new
            'vedoucí příslušník bezpečnostního sboru 1. řídící úrovně': police_entities,

            'vedoucí zastupitelského úřadu': 'Q121998',
        }

    def get(self, raw):
        name = raw.lower()
        return self.rulebook.get(name.strip())
