create table vn_party_name(party_id integer not null references vn_party(id),
	party_name varchar not null,
	unique(party_name));
