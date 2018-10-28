create table parse_error(url_id integer primary key references field(id),
       error_message varchar,
       failed timestamp not null);
