create table vn_reverse_field(url_id integer primary key references field(id),
       reverse_lowercase varchar not null);

create index idx_vn_reverse_field_reverse_lowercase on vn_reverse_field(reverse_lowercase);
