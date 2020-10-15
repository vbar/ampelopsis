create table locality(url_id integer primary key references field(id),
	instance_id integer references instances(id) not null);

create index idx_locality_instance on locality(instance_id);
