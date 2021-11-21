create table ast_person(id serial primary key,
        wikidata_id varchar not null,
        presentation_name varchar,
        unique(wikidata_id));
