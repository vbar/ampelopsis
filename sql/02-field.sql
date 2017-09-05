create table field(id serial primary key,
	url varchar(512) not null,
	checkd timestamp,
	unique(url));

