def list_persons(cur):
    names = []
    cur.execute("""select id, presentation_name
from ast_person
order by id""")
    rows = cur.fetchall()
    for person_id, person_name in rows:
        while len(names) < person_id:
            names.append(None)

        names.append(person_name)

    return names
