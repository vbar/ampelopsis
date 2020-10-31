create table tops(id serial primary key,
	hostname varchar not null,
	unique(hostname));
