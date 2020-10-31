create table volume_loc(volume_id integer primary key references directory(id),
	instance_id integer references instances(id) not null);

create index idx_volume_loc_instance on volume_loc(instance_id);
