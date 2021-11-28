create table ast_speech(speech_id integer references field(id) not null,
       speaker_id integer references ast_person(id),
       speech_day date,
       speech_order integer,
       word_count integer not null,
       content tsvector,
       unique(speech_id));

create index idx_ast_speech on ast_speech(speech_day);

create index idx_ast_speech_content on ast_speech using gin(content);
