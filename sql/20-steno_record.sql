create table steno_record(id serial primary key,
        hamlet_name varchar not null,
        presentation_name varchar,
        party_id integer references steno_party(id),
        card_url_id integer references field(id),
        unique(hamlet_name));
