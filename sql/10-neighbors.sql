create table neighbors(id serial primary key,
	url varchar not null,
	unique(url));
