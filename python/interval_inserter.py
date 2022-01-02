import dateparser

class IntervalInserter:
    def __init__(self, cur, interval_table, secondary_column, from_var, until_var):
        self.cur = cur
        self.interval_table = interval_table
        self.secondary_column = secondary_column
        self.from_var = from_var
        self.until_var = until_var

    def process(self, person_id, secondary, answer):
        from_date = self.get_date_literal(answer.get(self.from_var))
        until_date = self.get_date_literal(answer.get(self.until_var))
        if from_date:
            if until_date:
                self.insert_bounded(person_id, secondary, from_date, until_date)
            else:
                self.insert_starting(person_id, secondary, from_date)
        else:
            if until_date:
                self.insert_ending(person_id, secondary, until_date)
            else:
                self.insert_unbounded(person_id, secondary)

    def insert_unbounded(self, person_id, secondary):
        select = """select count(*)
from {table}
where person_id=%s and {column}=%s and from_date is null and until_date is null""".format(table=self.interval_table, column=self.secondary_column)
        self.cur.execute(select, (person_id, secondary))
        row = self.cur.fetchone()
        if not row[0]:
            insert = """insert into {table}(person_id, {column})
values(%s, %s)""".format(table=self.interval_table, column=self.secondary_column)
            self.cur.execute(insert, (person_id, secondary))
        # else the record is already there

    def insert_starting(self, person_id, secondary, from_date):
        select = """select id, from_date
from {table}
where person_id=%s and {column}=%s and from_date is not null and until_date is null""".format(table=self.interval_table, column=self.secondary_column)
        self.cur.execute(select, (person_id, secondary))
        rows = self.cur.fetchall()
        for interval_id, old_from in rows:
            if old_from > from_date:
                update = """update {table}
set from_date=%s
where id=%s""".format(table=self.interval_table)
                self.cur.execute(update, (from_date, interval_id))

            return

        insert = """insert into {table}(person_id, {column}, from_date)
values(%s, %s, %s)""".format(table=self.interval_table, column=self.secondary_column)
        self.cur.execute(insert, (person_id, secondary, from_date))

    def insert_ending(self, person_id, secondary, until_date):
        select = """select id, until_date
from {table}
where person_id=%s and {column}=%s and from_date is null and until_date is not null""".format(table=self.interval_table, column=self.secondary_column)
        self.cur.execute(select, (person_id, secondary))
        rows = self.cur.fetchall()
        for interval_id, old_until in rows:
            if old_until < until_date:
                update = """update {table}
set until_date=%s
where id=%s""".format(table=self.interval_table)
                self.cur.execute(update, (until_date, interval_id))

            return

        insert = """insert into {table}(person_id, {column}, until_date)
values(%s, %s, %s)""".format(table=self.interval_table, column=self.secondary_column)
        self.cur.execute(insert, (person_id, secondary, until_date))

    def insert_bounded(self, person_id, secondary, from_date, until_date):
        assert from_date
        assert until_date

        if from_date > until_date:
            print("ignoring statement with switched bounds", file=sys.stderr)
            return

        select = """select id, from_date, until_date
from {table}
where person_id=%s and {column}=%s and from_date is not null and until_date is not null
order by from_date, until_date""".format(table=self.interval_table, column=self.secondary_column)
        self.cur.execute(select, (person_id, secondary))
        rows = self.cur.fetchall()
        last_id = None
        for interval_id, sweep_from, sweep_until in rows:
            if sweep_from > sweep_until:
                raise Exception("invalid interval")

            if (from_date <= sweep_from) and (until_date >= sweep_from):
                if (last_id is not None) and ((from_date != sweep_from) or (until_date != sweep_until)):
                    delete = """delete
from {table}
where id=%s""".format(table=self.interval_table)
                    self.cur.execute(delete, (last_id,))

                if until_date < sweep_until:
                    until_date = sweep_until

                if (from_date != sweep_from) or (until_date != sweep_until):
                    update = """update {table}
set from_date=%s, until_date=%s
where id=%s""".format(table=self.interval_table)
                    self.cur.execute(update, (from_date, until_date, interval_id))

                last_id = interval_id

        if last_id is None:
            insert = """insert into {table}(person_id, {column}, from_date, until_date)
values(%s, %s, %s, %s)""".format(table=self.interval_table, column=self.secondary_column)
            self.cur.execute(insert, (person_id, secondary, from_date, until_date))

    @staticmethod
    def get_date_literal(d):
        if not d:
            return None

        if not isinstance(d, dict):
            raise Exception("unexpected SPARQL format")

        tp = d.get('type')
        if tp == 'uri': # apparently used for values that went away...
            return None

        if tp != 'literal':
            raise Exception("unexpected literal variable format: " + tp)

        datatype = d.get('datatype')
        if datatype != 'http://www.w3.org/2001/XMLSchema#dateTime':
            raise Exception("unexpected date type " + datatype)

        v = d.get('value')
        if not v:
            return None

        dt = dateparser.parse(v)
        if not dt:
            return None

        # actually can have even lower precision, but we're ignoring
        # that (so far)...
        return dt.date()
