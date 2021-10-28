create table steno_party_name(party_id integer not null references steno_party(id),
        party_name varchar not null,
        unique(party_name));
