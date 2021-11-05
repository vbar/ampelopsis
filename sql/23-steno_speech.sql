create table steno_speech(speech_id integer references field(id) not null,
       speaker_id integer references steno_record(id),
       speech_day date,
       speech_order integer,
       word_count integer not null,
       content tsvector,
       unique(speech_id));

create index idx_steno_speech on steno_speech(speech_day);

create index steno_speech_content_idx on steno_speech using gin(content);
