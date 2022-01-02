create table ast_person_position(id serial primary key,
        person_id integer not null references ast_person(id),
        wikidata_id varchar not null,
        from_date date,
        until_date date);
