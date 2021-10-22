create table rap_submitters(id serial primary key,
        submitter varchar not null,
        unique(submitter));
