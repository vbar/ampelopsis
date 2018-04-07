import configparser
import os
import psycopg2
import re
from urllib.parse import quote_plus

def get_parent_directory():
    cur_dir = os.path.dirname(__file__)
    return os.path.dirname(cur_dir)

config = configparser.ConfigParser()
ini_path = os.path.join(get_parent_directory(), "ampelopsis.ini")
config.read(ini_path)

def get_option(name, default_value):
    return config.get("root", name) if config.has_option("root", name) else default_value

def get_mandatory_option(name):
    return config.get("root", name)

schema = get_option("schema", "")

def get_volume_path(volume_id):
    data_dir = os.path.join(get_parent_directory(), "data")

    if schema:
        data_dir = os.path.join(data_dir, schema)

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    name = "%d.zip" % (volume_id,)
    return os.path.join(data_dir, name)

def get_loose_path(url_id, hdr=False):
    tmp_dir = os.path.join(get_parent_directory(), "tmp")
    
    if schema:
        tmp_dir = os.path.join(tmp_dir, schema)
        
    middle = str(url_id % 1000)
    loose_dir = os.path.join(tmp_dir, middle)
    if not os.path.exists(loose_dir):
        os.makedirs(loose_dir)

    name = str(url_id)
    if hdr:
        name += 'h'

    return os.path.join(loose_dir, name)

def make_connection():
    conn = psycopg2.connect(dbname='ampelopsis', host=get_option('dbhost', 'localhost'), user=get_mandatory_option('dbuser'), password=get_mandatory_option('dbpass'))
    conn.autocommit = True

    if schema:
        with conn.cursor() as cur:
            cur.execute("set search_path to " + schema)
            
    return conn

def get_netloc(pr):
    if (pr.username is None) and (pr.password is None): # keep it simple
        if ((pr.scheme == 'http') and (pr.port == 80)) or ((pr.scheme == 'https') and (pr.port == 443)):
            return pr.hostname

    return pr.netloc

# we want to be conservative, but a space is a space...
space_rx = re.compile('%20')

# www.realhit.cz uses accents in URLs...
def normalize_url_component(path):
    q = quote_plus(path, safe="/+%&=[]:")
    return space_rx.sub('+', q)
