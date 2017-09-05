create table extra(url_id integer references field(id) primary key,
       has_body boolean not null default(true),
       hash char(40),
       siz integer);

create index idx_extra_hash on extra(hash);

