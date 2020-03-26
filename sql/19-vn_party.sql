create table vn_party(id serial primary key,
	short_name varchar not null,
	unique(short_name));
