create table extra(url_id integer references field(id) primary key,
       content_type varchar(64),
       has_body boolean not null default(true),
       hash char(40),
       siz integer);

create index idx_extra_hash on extra(hash);

