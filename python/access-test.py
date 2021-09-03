#!/usr/bin/python3

import sys
from common import make_connection
from json_lookup import JsonLookup

class Accumulator(JsonLookup):
    def __init__(self, cur, dump_value, white):
        JsonLookup.__init__(self, cur)
        self.dump_value = dump_value
        self.white = None if white is None else set(white)
        self.flag2count = {}
        self.checked = 0

    def is_checked(self, doc):
        if self.white is None:
            return True

        position = self.make_position_set(doc)
        return len(self.white & position)

    def accumulate(self, doc):
        self.checked += 1

        statements = doc.get('statements')
        if statements and len(statements):
            flag_set = set()
            for stm in statements:
                raw_flag = stm.get('canOpen')
                flag = str(raw_flag)
                if (self.dump_value is not None) and (flag == self.dump_value):
                    return True
                else:
                    flag_set.add(flag)

            flags = sorted(flag_set)
            k = " / ".join(flags)
            cnt = self.flag2count.get(k, 0)
            self.flag2count[k] = cnt + 1

        return False

    def run(self):
        self.cur.execute("""select url
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            url = row[0]
            doc = self.get_document(url)
            if self.is_checked(doc):
                if self.dump_value is None:
                    print(url + "...", file=sys.stderr)

                if self.accumulate(doc):
                    print(url)

    def dump(self):
        if self.dump_value is not None:
            return

        print("checked %d documents:" % self.checked)
        flags = sorted(self.flag2count.keys(), key=lambda k: -1 * self.flag2count[k])
        for flag in flags:
            print("%d x %s" % (self.flag2count[flag], flag))


def main():
    l = len(sys.argv)
    dump_value = None
    tail_idx = 1
    if l > 1 and sys.argv[1] in ('-d', '--dump'):
        if l < 3:
            raise Exception("missing argument")

        dump_value = sys.argv[2]
        tail_idx = 3

    if l > tail_idx:
        whitelist = sys.argv[tail_idx:]
    else:
        whitelist = None

    with make_connection() as conn:
        with conn.cursor() as cur:
            accu = Accumulator(cur, dump_value, whitelist)
            accu.run()
            accu.dump()

if __name__ == "__main__":
    main()
