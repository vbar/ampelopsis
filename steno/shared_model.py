def list_persons(cur):
    names = []
    colors = []
    cur.execute("""select steno_record.id, presentation_name, color
from steno_record
left join steno_party on steno_party.id=steno_record.party_id
order by id""")
    rows = cur.fetchall()
    for person_id, person_name, party_color in rows:
        while len(names) < person_id:
            names.append("")

        while len(colors) < person_id:
            colors.append("")

        names.append(person_name)

        bare_color = party_color or 'AAA'
        colors.append('#' + bare_color)

    return (names, colors)
