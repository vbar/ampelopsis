# Wikidata entities named in more than one place (i.e. in both
# rulebook and jumper). Entities named just once (i.e. to initialize a
# position set, which is then used without special-casing the entity)
# generally just hardcode the Wikidata name.

# ministers are special because their position is a subclass of
# minister - normally minister of <resort> of Czech Republic
minister_position_entity = 'Q83307'

# MPs are special because we want to check their terms. More generic
# Q486839 is also occasionally used, but apparently never without
# Q19803234, so we ignore it.
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

# Judges, policemen, doctors and researchers are special because
# (unlike other matched persons) they aren't
# politicians. Constitutional judge (Q59773473) is also used, but
# doesn't match anybody new - it's enough to match the description...
judge_position_entity = 'Q16533'
police_officer_position_entity = 'Q384593'
physician_position_entity = 'Q39631'
psychiatrist_position_entity = 'Q211346'
researcher_position_entity = 'Q1650915'

# Prosecutor not only isn't a position; it may not be linked at all.
# We try to (also) match it from description.
prosecutor_position_entity = 'Q600751'

# not particularly special but do repeat
deputy_minister_position_entity = 'Q15735113'
director_position_entity = 'Q1162163'
region_councillor_position_entity = 'Q27830328'
rector_of_charles_university_position_entity = 'Q12049166'
