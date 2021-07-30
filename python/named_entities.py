# Wikidata entities named in more than one place (i.e. in both
# rulebook and jumper). Entities named just once (i.e. to initialize a
# position set, which is then used without special-casing the entity)
# generally just hardcode the Wikidata name.
class Entity:
    # so far used only for police "president", but there might be
    # others...
    president = 'Q30461'

    # ministers are special because their position is a subclass of
    # minister - normally minister of <resort> of Czech Republic
    minister = 'Q83307'

    # MPs are special because we want to check their terms. More
    # generic Q486839 is also occasionally used, but apparently never
    # without Q19803234, so we ignore it.
    mp = 'Q19803234'

    # input data don't distinguish parliamentary functions, but
    # wikidata (at least occasionally) do...
    mp_speaker = 'Q5068060'

    # Ambassadors are special because they're normally ambassadors
    # somewhere - IOW their position is a subclass of ambassador.
    ambassador = 'Q121998'

    # Judges, policemen, doctors etc. are special because (unlike
    # other matched persons) they aren't politicians.
    judge = 'Q16533'
    constitutional_judge = 'Q59773473'
    diplomat = 'Q193391'
    police_officer = 'Q384593'
    physician = 'Q39631'
    psychiatrist = 'Q211346'
    hygienist = 'Q651566'
    veterinarian = 'Q202883'
    archaeologist = 'Q3621491'
    academic = 'Q3400985'
    researcher = 'Q1650915'
    engineer = 'Q81096'
    manager = 'Q2462658'

    # A specific civil servant; the entity isn't just a position, but
    # may also occur in statements linking it to the office.
    head_of_office_of_government = 'Q15712674'

    # Q462390 "docent" is also possible, but isn't that common (to be
    # expected, considering it's no longer issued), doesn't match
    # anybody new and causes timeouts, too...
    university_teacher = 'Q1622272'

    # Prosecutor not only isn't a position; it may not be linked at all.
    # We try to (also) match it from description.
    prosecutor = 'Q600751'

    # State's attorney is treated as a synonym of prosecutor.
    state_attorney = 'Q10726370'

    # like researcher, but also applies to lower-level/private schools
    pedagogue = 'Q1231865'
    teacher = 'Q37226'

    # most matched persons are politicians (and we don't generally
    # require this to match them), but it's used to filter common
    # occupations
    politician = 'Q82955'

    # not particularly special but do repeat
    chairperson = 'Q140686'
    director = 'Q1162163'
    rector_of_charles_university = 'Q12049166'

deputy_minister_position_entities = (
    # "Assistant Secretary" is actually used...
    'Q15735113',

    'Q26204040'
)

# mayors are special because there's so many the name match may
# produce false positives - so for mayors we also match the city
mayor_position_entities = ( 'Q30185', 'Q147733', 'Q99356295' )

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
    'Q97482814',
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
