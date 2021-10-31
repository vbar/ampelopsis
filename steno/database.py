from flask import g
import functools
import os
from psycopg2 import pool
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'python'))

from common import get_mandatory_option, get_option, schema

the_pool = pool.ThreadedConnectionPool(1, 4, dbname='ampelopsis', host=get_option('dbhost', 'localhost'), user=get_mandatory_option('dbuser'), password=get_mandatory_option('dbpass'))

def get_connection():
    conn = the_pool.getconn()
    conn.autocommit = True

    if schema:
        with conn.cursor() as cur:
            cur.execute("set search_path to " + schema)

    return conn

def release_connection(conn):
    the_pool.putconn(conn)

def databased(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        g.conn = get_connection()
        rv = f(*args, **kwargs)
        release_connection(g.conn)
        g.conn = None
        return rv

    return wrapper
