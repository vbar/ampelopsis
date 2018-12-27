import re

from levels import CouncilLevel, DirectorLevel, JudgeLevel, MuniLevel, ParliamentLevel, UniversityLevel
from named_entities import councillor_position_entities, deputy_mayor_position_entities, deputy_minister_position_entity, director_position_entity, mayor_position_entities, minister_position_entity, mp_position_entity, physician_position_entity, police_officer_position_entity, psychiatrist_position_entity, rector_of_charles_university_position_entity, region_councillor_position_entity
from rulebook_util import get_org_name

# Mostly generic. Prague is not included because it is a city, and is
# handled on a higher level (not using
# region_councillor_position_entity - perhaps it should?)
region2councillor = {
    'jihočeský kraj': ( 'Q55670007', region_councillor_position_entity ),
    'jihomoravský kraj': ( 'Q59583668', region_councillor_position_entity ),
    'karlovarský kraj': region_councillor_position_entity,
    'kraj vysočina': region_councillor_position_entity,
    'krajský úřad středočeského kraje': region_councillor_position_entity,
    'královéhradecký kraj': ( 'Q59539134', region_councillor_position_entity ),
    'liberecký kraj': region_councillor_position_entity,
    'moravskoslezský kraj': ( 'Q55973189', region_councillor_position_entity ),
    'olomoucký kraj': region_councillor_position_entity,
    'pardubický kraj': region_councillor_position_entity,
    'plzeňský kraj': region_councillor_position_entity,
    'středočeský kraj': region_councillor_position_entity,
    'ústecký kraj': region_councillor_position_entity,
    'zlínský kraj': region_councillor_position_entity,
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
    'kancelář prezidenta republiky': 'Q15712674',
    'psychiatrická nemocnice bohnice': psychiatrist_position_entity,
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
        police_entities = ( director_position_entity, police_officer_position_entity )

        self.rulebook = {
            # "governing body member" doesn't sound very university-specific,
            # but is actually used either for universities, or regional
            # councils/obscure orgs not found in Wikidata
            'člen řídícího orgánu': UniversityLevel(),

            'vedoucí zaměstnanec 3. stupně řízení': director_level,
            'vedoucí zaměstnanec 4. stupně řízení': director_level,

            # apparently doesn't include deputy ministers (but does include
            # premier)
            'člen vlády': minister_position_entity,

            'náměstek člena vlády': deputy_minister_position_entity,
            'náměstek pro řízení sekce': deputy_minister_position_entity,
            'poslanec': ParliamentLevel(mp_position_entity),
            'senátor': 'Q18941264',
            'starosta': MuniLevel(mayor_position_entities),
            'místostarosta / zástupce starosty': MuniLevel(deputy_mayor_position_entities),
            'člen zastupitelstva': council_level,
            'člen rady': council_level,

            # not clear whether input distinguishes member from governor, so
            # we don't - there shouldn't be so many of them anyway...
            'člen bankovní rady české národní banky': ( 'Q28598459', 'Q25505764' ),

            'soudce': JudgeLevel(),
            'ředitel bezpečnostního sboru': police_entities,
            # ředitel odboru/sekce doesn't match any more directors
            'vedoucí příslušník bezpečnostního sboru 1. řídící úrovně': police_entities,
            'vedoucí zastupitelského úřadu': 'Q121998',
        }

    def get(self, raw):
        name = raw.lower()
        return self.rulebook.get(name.strip())
