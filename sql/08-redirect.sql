create table redirect(from_id integer references field(id),
       to_id integer references field(id),
       primary key(from_id, to_id));
