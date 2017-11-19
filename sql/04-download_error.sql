create table download_error(url_id integer primary key references field(id),
       error_code integer not null,
       error_message varchar,
       failed timestamp not null);
       
