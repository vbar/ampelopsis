create table vn_identity(hamlet_name varchar not null,
       town_name varchar not null,
       claim_count integer not null,
       primary key (hamlet_name, town_name));
