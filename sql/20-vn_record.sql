create table vn_record(id serial primary key,
	hamlet_name varchar not null,
	presentation_name varchar,
	party_id integer references vn_party(id),
	unique(hamlet_name));
