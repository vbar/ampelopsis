create table ast_identity_card(person_id integer references ast_person(id),
       link_id integer references field(id),
       primary key (person_id, link_id));
