create table multiple(id1 integer references field(id),
       id2 integer references field(id),
       primary key(id1, id2),
       check(id1 < id2));
       
