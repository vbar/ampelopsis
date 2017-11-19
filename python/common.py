import configparser
import os
import psycopg2

def get_volume_path(volume_id):
    data_dir = os.path.join(get_parent_directory(), "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    name = "%d.zip" % (volume_id,)
    return os.path.join(data_dir, name)

def get_loose_path(url_id, hdr=False):
    tmp_dir = os.path.join(get_parent_directory(), "tmp")
    middle = str(url_id % 1000)
    loose_dir = os.path.join(tmp_dir, middle)
    if not os.path.exists(loose_dir):
        os.makedirs(loose_dir)

    name = str(url_id)
    if hdr:
        name += 'h'

    return os.path.join(loose_dir, name)

def get_parent_directory():
    cur_dir = os.path.dirname(__file__)
    return os.path.dirname(cur_dir)

def get_option(name, default_value):
    return config.get("root", name) if config.has_option("root", name) else default_value

def get_mandatory_option(name):
    return config.get("root", name)

def make_connection():
    conn = psycopg2.connect(dbname='ampelopsis', host=get_option('dbhost', 'localhost'), user=get_mandatory_option('dbuser'), password=get_mandatory_option('dbpass'))
    conn.autocommit = True
    return conn

config = configparser.ConfigParser()
ini_path = os.path.join(get_parent_directory(), "ampelopsis.ini")
config.read(ini_path)

