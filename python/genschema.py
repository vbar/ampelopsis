#!/usr/bin/python3

import os
from common import get_parent_directory, schema

def main():
    if not schema:
        print("no schema is configured")
        return

    sql_dir = os.path.join(get_parent_directory(), "sql")
    script = os.path.join(sql_dir, "00-schema.sql")
    with open(script, 'w') as f:
        f.write("create schema %s;\n" % schema)
        f.write("set search_path to %s;\n" % schema)

if __name__ == "__main__":
    main()

