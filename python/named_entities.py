# Wikidata entities named in more than one place (i.e. in both
# rulebook and jumper). Entities named just once (i.e. to initialize a
# position set, which is then used without special-casing the entity)
# generally just hardcode the Wikidata name.
class Entity:
    # ministers are special because their position is a subclass of
    # minister - normally minister of <resort> of Czech Republic
    minister = 'Q83307'

    # MPs are special because we want to check their terms. More
    # generic Q486839 is also occasionally used, but apparently never
    # without Q19803234, so we ignore it.
    mp = 'Q19803234'

    # Judges, policemen, doctors and researchers are special because
    # (unlike other matched persons) they aren't
    # politicians. Constitutional judge (Q59773473) is also used, but
    # doesn't match anybody new - it's enough to match the
    # description...
    judge = 'Q16533'
    police_officer = 'Q384593'
    physician = 'Q39631'
    psychiatrist = 'Q211346'
    researcher = 'Q1650915'

    # Prosecutor not only isn't a position; it may not be linked at all.
    # We try to (also) match it from description.
    prosecutor = 'Q600751'

    # like researcher, but also applies to lower-level/private schools
    pedagogue = 'Q1231865'

    # not particularly special but do repeat
    deputy_minister = 'Q15735113'
    director = 'Q1162163'
    rector_of_charles_university = 'Q12049166'

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

region_councillor_entities = (
    # generic "zastupitel kraje" rarely if ever matches anything
    'Q27830328',

    # "hejtman" is a possible alternative
    'Q11027282',

    # "náměstek hejtmana" is probably the only entity in this list
    # that actually matches someone new...
    'Q30103103'
)
