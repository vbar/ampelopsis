create table ast_person(id serial primary key,
        presentation_name varchar not null,
        birth_year int,
        position_list varchar,
        wikidata_id varchar not null,
        unique(wikidata_id));
