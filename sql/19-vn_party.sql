create table vn_party(id serial primary key,
	long_name varchar not null,
	short_name varchar,
	color char(6),
	unique(long_name),
	unique(short_name));
