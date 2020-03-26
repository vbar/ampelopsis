create table vn_identity_hamlet(record_id integer references vn_record(id),
       town_name varchar not null,
       primary key (record_id, town_name));
