create or replace function get_priority(url varchar)
        returns integer as
$BODY$
begin
	return length(url) - length(replace(url, '/', '')) + 5 * (length(url) - length(regexp_replace(url, '[?&]', '', 'g')));
end;
$BODY$
language 'plpgsql';
