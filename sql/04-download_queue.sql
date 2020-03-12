-- https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/
create table download_queue(url_id integer primary key,
	priority integer not null,
	host_id integer not null);

create index idx_download_queue_priority on download_queue(priority);

create index idx_download_queue_host_id on download_queue(host_id);
