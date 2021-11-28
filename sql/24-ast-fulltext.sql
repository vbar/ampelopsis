-- based on https://www.postgresql.org/docs/13/textsearch-configuration.html

create text search configuration ast_config ( copy = pg_catalog.english );

create text search dictionary ast_simple_dict ( template = pg_catalog.simple );

alter text search configuration ast_config
      alter mapping for asciiword, asciihword, hword_asciipart, word, hword, hword_part with ast_simple_dict;

alter text search configuration ast_config
      drop mapping for email, url, url_path, sfloat, float;
