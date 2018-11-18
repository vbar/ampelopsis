import re

# ministers are special because their position is a subclass of
# minister - normally minister of <resort> of Czech Republic
minister_position_entity = 'Q83307'

# mayors are special because there's so many the name match may
# produce false positives - so for mayors we also match the city
mayor_position_entities = ( 'Q30185', 'Q147733' )

# handled like a councillor, except it has its own working position
# name in rulebook
deputy_mayor_position_entity = 'Q581817'

# councillors are even commoner than mayors
councillor_position_entities = ( 'Q708492', 'Q19602879', 'Q4657217' )

# judges are special because (unlike other matched persons) they
# aren't politicians
judge_position_entity = 'Q16533'

# not particularly special but does repeat
director_position_entity = 'Q1162163'

university_name_rx = re.compile("\\b(?:univerzita|učení)")

university2rector = {
    'Univerzita Karlova': 'Q12049166',
    'Univerzita Karlova v Praze': 'Q12049166'
}

# Correct positions mostly not found - better search is probably
# needed... Prague is not included because it is a city, and is
# handled on a higher level.
region2councillor = {
    'jihočeský kraj': 'Q55670007',
    'jihomoravský kraj': False,
    'karlovarský kraj': False,
    'kraj vysočina': False,
    'královéhradecký kraj': False,
    'liberecký kraj': False,
    'moravskoslezský kraj': 'Q55973189',
    'olomoucký kraj': False,
    'pardubický kraj': False,
    'plzeňský kraj': False,
    'středočeský kraj': False,
    'ústecký kraj': False,
    'zlínský kraj': False,
}

def get_org_name(it):
    return it['organization'].strip()

# A rulebook (q.v.) value marking the match as a position that should
# also match a city/village. Note that instances of this object can be
# values of multiple rulebook keys, but each individual instance must
# be initialized with a non-overlapping position set (technically an
# iterable, or a single string).
class CityLevel:
    def __init__(self, positions):
        self.positions = positions

    def __call__(self, it):
        return self.positions

# Match for a city council, or some other council; currently we just
# check regions.
class CouncilLevel(CityLevel):
    def __init__(self, default_level):
        self.default_level = default_level

    def __call__(self, it):
        org_name = get_org_name(it)
        pos = region2councillor.get(org_name.lower())
        if pos is None:
            return self.default_level(it)
        else:
            # this match isn't for a city and doesn't contribute to
            # city set
            return pos if pos else []

def produce_academic(it):
    org_name = get_org_name(it)

    sought = []
    rector = university2rector.get(org_name)
    if rector:
        sought.append(rector)

    if rector or university_name_rx.search(org_name.lower()):
        sought.extend([ 'Q212071', 'Q2113250', 'Q723682' ])

    return sought

def produce_director(it):
    org_name = get_org_name(it)

    if (org_name == 'Kancelář prezidenta republiky'):
        return 'Q15712674'
    else:
        return director_position_entity

councillor_city_level = CityLevel(councillor_position_entities)

# Maps it['workingPosition']['name'], where it is an item of cro
# detail page JSON attribute 'workingPositions', to position set. The
# mapping can be a single string, an iterable, or a callable returning
# a string or an iterable. The callable is called with the it value.
rulebook = {
    'člen řídícího orgánu': produce_academic,
    'vedoucí zaměstnanec 3. stupně řízení': produce_director,
    'člen vlády': minister_position_entity, # apparently doesn't include deputy ministers (but does include premier)
    'náměstek pro řízení sekce': 'Q15735113',
    'starosta': CityLevel(mayor_position_entities),
    'místostarosta / zástupce starosty': CityLevel(deputy_mayor_position_entity),
    'člen zastupitelstva': councillor_city_level,
    'člen Rady': CouncilLevel(councillor_city_level),
    'člen bankovní rady České národní banky': ( 'Q28598459', 'Q25505764' ), # not clear whether input distinguishes member from governor, so we don't - there shouldn't be so many of them anyway...
    'soudce': judge_position_entity,
    'ředitel bezpečnostního sboru': director_position_entity,
    'vedoucí zastupitelského úřadu': 'Q121998',
}
