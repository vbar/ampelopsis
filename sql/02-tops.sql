create table tops(id serial primary key,
	hostname varchar not null,
	instance_id integer references instances(id),
	unique(hostname));
