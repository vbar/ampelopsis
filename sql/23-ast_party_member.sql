create table ast_party_member(id serial primary key,
       person_id integer not null references ast_person(id),
       party_id integer not null references ast_party(id),
       from_date date,
       until_date date);
