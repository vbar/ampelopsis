import common


def make_set_search_path(sch):
    return 'set search_path to ' + sch


class SchemaManager:
    def __init__(self, context_schema, cur):
        self.context_schema = context_schema
        self.context_command = make_set_search_path(context_schema)
        self.cur = cur

    def __enter__(self):
        self.default_schema = common.schema
        self.default_command = make_set_search_path(self.default_schema)
        common.schema = self.context_schema
        self.cur.execute(self.context_command)
        return self

    def __exit__(self, exc_type, value, traceback):
        common.schema = self.default_schema
        self.cur.execute(self.default_command)
        return False
