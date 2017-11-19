create table content(url_id integer primary key references field(id),
	volume_id integer references directory(id));
       
create index idx_content_volume on content(volume_id);
