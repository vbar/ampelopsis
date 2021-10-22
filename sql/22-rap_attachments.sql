create table rap_attachments(att_id integer references field(id) not null,
       doc_id integer references rap_documents(doc_id) not null,
       att_day date,
       att_type varchar,
       att_no integer,
       word_count integer not null,
       unique(att_id));
