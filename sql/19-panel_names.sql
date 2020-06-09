create table panel_names(id serial primary key,
	town_name varchar not null,
	unique(town_name));
