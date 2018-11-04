create table field_cache(id integer not null,
	url varchar not null,
	checkd timestamp,
	unique(id),
	unique(url));
