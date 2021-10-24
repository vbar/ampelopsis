create table rap_attachments(att_id integer references field(id) not null,
       doc_id integer references rap_documents(doc_id) not null,
       att_day date,
       att_type varchar,
       att_no integer,
       word_count integer not null,
       content tsvector,
       unique(att_id));

create index rap_attachments_content_idx on rap_attachments using gin(content);
