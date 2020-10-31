create table host_inst(host_id integer references tops(id),
	instance_id integer references instances(id),
	unique(host_id, instance_id));
