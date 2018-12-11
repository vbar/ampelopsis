import re

from levels import CouncilLevel, MuniLevel, ParliamentLevel, UniversityLevel
from named_entities import councillor_position_entities, deputy_mayor_position_entities, deputy_minister_position_entity, director_position_entity, judge_position_entity, mayor_position_entities, minister_position_entity, mp_position_entity, rector_of_charles_university_position_entity, region_councillor_position_entity
from rulebook_util import get_org_name

# Mostly generic. Prague is not included because it is a city, and is
# handled on a higher level (not using
# region_councillor_position_entity - perhaps it should?)
region2councillor = {
    'jihočeský kraj': ( 'Q55670007', region_councillor_position_entity ),
    'jihomoravský kraj': region_councillor_position_entity,
    'karlovarský kraj': region_councillor_position_entity,
    'kraj vysočina': region_councillor_position_entity,
    'krajský úřad středočeského kraje': region_councillor_position_entity,
    'královéhradecký kraj': region_councillor_position_entity,
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

def produce_director(it):
    org_name = get_org_name(it)

    if (org_name == 'kancelář prezidenta republiky'):
        return 'Q15712674'
    else:
        return director_position_entity

council_level = CouncilLevel(unknown_council_set, region2councillor, MuniLevel(councillor_position_entities))

# Maps it['workingPosition']['name'], where it is an item of cro
# detail page JSON attribute 'workingPositions', to position set. The
# mapping can be a single string, an iterable, or a callable returning
# a string or an iterable. The callable is called with the it value.
rulebook = {
    'člen řídícího orgánu': UniversityLevel(),
    'vedoucí zaměstnanec 3. stupně řízení': produce_director,
    'člen vlády': minister_position_entity, # apparently doesn't include deputy ministers (but does include premier)
    'náměstek člena vlády': deputy_minister_position_entity,
    'náměstek pro řízení sekce': deputy_minister_position_entity,
    'poslanec': ParliamentLevel(mp_position_entity),
    'senátor': 'Q18941264',
    'starosta': MuniLevel(mayor_position_entities),
    'místostarosta / zástupce starosty': MuniLevel(deputy_mayor_position_entities),
    'člen zastupitelstva': council_level,
    'člen Rady': council_level,
    'člen bankovní rady České národní banky': ( 'Q28598459', 'Q25505764' ), # not clear whether input distinguishes member from governor, so we don't - there shouldn't be so many of them anyway...
    'soudce': judge_position_entity,
    'ředitel bezpečnostního sboru': director_position_entity,
    'vedoucí příslušník bezpečnostního sboru 1. řídící úrovně': director_position_entity,
    'vedoucí zastupitelského úřadu': 'Q121998',
}
