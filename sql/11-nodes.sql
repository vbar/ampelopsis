create table nodes(url_id integer references field(id) primary key,
	depth integer,
	yield real);
