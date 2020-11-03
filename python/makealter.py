#!/usr/bin/python3

import os
import shutil
import sys

# can't be imported from common because common requires existing
# config file
def get_parent_directory():
    cur_dir = os.path.dirname(__file__)
    return os.path.dirname(cur_dir)


def ensure_dir_link(primary_dir, sub_name):
    subdir = os.path.join(primary_dir, sub_name)
    if not os.path.exists(subdir):
        os.makedirs(subdir)

    alter_dir = os.path.join(get_parent_directory(), sub_name)
    os.symlink(subdir, alter_dir)


def main():
    if len(sys.argv) != 2:
        raise Exception("usage: %s directory" % sys.argv[0])

    primary_dir = os.path.abspath(sys.argv[1])
    if not os.path.isdir(primary_dir):
        raise Exception("%s is not a directory" % primary_dir)

    primary_config = os.path.join(primary_dir, "ampelopsis.ini")
    if not os.path.isfile(primary_config): # don't make alters of alters
        raise Exception("primary config file %s not found" % primary_config)

    ensure_dir_link(primary_dir, "data")
    ensure_dir_link(primary_dir, "tmp")

    alter_config = os.path.join(get_parent_directory(), "ampelopsis.ini")
    if os.path.exists(alter_config):
        print("local config already exists - won't update", file=sys.stderr)
    else:
        shutil.copyfile(primary_config, alter_config)


if __name__ == "__main__":
    main()
