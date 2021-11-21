create table ast_party_name(party_id integer not null references ast_party(id),
        party_name varchar not null,
        unique(party_name));
