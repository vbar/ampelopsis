create table steno_speech(speech_id integer references field(id) not null,
       speaker_id integer references steno_record(id),
       speech_day date,
       speech_order integer,
       word_count integer not null,
       unique(speech_id));

create index idx_steno_speech on steno_speech(speech_day);
