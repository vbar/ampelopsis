create table instances(id serial primary key,
	instance_name varchar not null,
	unique(instance_name));
