create table rap_documents(doc_id integer references field(id) not null,
       pid varchar,
       submitter_id integer references rap_submitters(id),
       doc_day date,
       doc_name varchar,
       doc_desc varchar,
       unique(doc_id));
