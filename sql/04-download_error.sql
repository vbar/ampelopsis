create table download_error(url_id integer primary key references field(id),
       error_code integer, -- NULL from curl seen for 403
       error_message varchar,
       failed timestamp not null);
       
