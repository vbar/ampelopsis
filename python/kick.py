#!/usr/bin/python3

from act_util import act_reset
from common import get_option, make_connection

class Kicker:
    def __init__(self, cur):
        self.cur = cur
        self.inst_name = get_option("instance", None)

    def run(self):
        # leaf URLs are considered downloaded (and parsed, unless
        # somebody clears the parsed state) but may not have any
        # locality - in which case they'd get stuck in parse_queue and
        # must not be inserted there at all...
        loc_join = "join locality on field.id=locality.url_id" if self.inst_name else ""
        sql = """insert into parse_queue(url_id)
select field.id
from field
%s
left join parse_queue on field.id=parse_queue.url_id
where checkd is not null and parsed is null and parse_queue.url_id is null""" % loc_join
        self.cur.execute(sql)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            # maybe check here whether download and/or parse is running? it shouldn't...
            act_reset(cur)

            kicker = Kicker(cur)
            kicker.run()

if __name__ == "__main__":
    main()
