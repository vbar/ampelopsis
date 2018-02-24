create table field(id serial primary key,
	url varchar not null,
	checkd timestamp,
	parsed timestamp,
	unique(url));

