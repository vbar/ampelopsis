create table ast_party(id serial primary key,
        wikidata_id varchar not null,
        color char(6),
        unique(wikidata_id));
