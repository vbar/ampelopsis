# Wikidata entities named in more than one place (i.e. in both
# rulebook and jumper). Entities named just once (i.e. to initialize a
# position set, which is then used without special-casing the entity)
# generally just hardcode the Wikidata name.

# ministers are special because their position is a subclass of
# minister - normally minister of <resort> of Czech Republic
minister_position_entity = 'Q83307'

# MPs are special because we want to check their terms
mp_position_entity = 'Q19803234'

# mayors are special because there's so many the name match may
# produce false positives - so for mayors we also match the city
mayor_position_entities = ( 'Q30185', 'Q147733' )

# handled like a councillor, except it has its own working position
# name in rulebook
deputy_mayor_position_entities = (
    'Q581817',

    # "Wali" doesn't sound very applicable (and probably isn't very specific), but it is used...
    'Q13424814'
)

# councillors are even commoner than mayors
councillor_position_entities = (
    'Q708492',
    'Q4657217',

    # "representative" might match both on municipal and region level,
    # but trying to use it on region level didn't match anybody new
    # (regional politicians apparently tend to have other positions as
    # well), so for now we don't bother
    'Q19602879'
)

# judges are special because (unlike other matched persons) they
# aren't politicians
judge_position_entity = 'Q16533'

# not particularly special but do repeat
deputy_minister_position_entity = 'Q15735113'
director_position_entity = 'Q1162163'
region_councillor_position_entity = 'Q27830328'
