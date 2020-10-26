#!/usr/bin/python3

import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from psycopg2 import pool
import re
import shutil
import sys
from common import get_mandatory_option, get_option, schema
from storage_bridge import StorageBridge
from volume_bridge import VolumeBridge

the_pool = pool.ThreadedConnectionPool(database='ampelopsis', host=get_option('dbhost', 'localhost'), user=get_mandatory_option('dbuser'), password=get_mandatory_option('dbpass'), minconn=0, maxconn=int(get_option('own_max_num_conn', "4")))

def get_connection():
    conn = the_pool.getconn()
    conn.autocommit = True

    if schema:
        with conn.cursor() as cur:
            cur.execute("set search_path to " + schema)

    return conn


def get_path_rx():
    id_rx_group = "([0-9]{1,10})(|h|[.]zip)"
    if schema:
        path_rx = re.compile("^/" + re.escape(schema) + "/" + id_rx_group + "$")
    else:
        path_rx = re.compile("^/" + id_rx_group + "$")

    return path_rx


class StorageHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not self.path or (self.path == "/"):
            self._serve_root()
            return

        path_rx = get_path_rx()
        m = path_rx.match(self.path)
        if not m:
            self.send_error(404, "Path not found")
            return

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                obj_id = int(m.group(1))
                ext = m.group(2)
                if ext == '.zip':
                    self._serve_volume(cur, obj_id)
                else:
                    self._serve(cur, obj_id, ext)
        finally:
            the_pool.putconn(conn)

    def do_DELETE(self):
        path_rx = get_path_rx()
        m = path_rx.match(self.path)
        if not m:
            self.send_error(404, "Path not found")
            return

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                obj_id = int(m.group(1))
                ext = m.group(2)
                if ext == '.zip':
                    self._delete_volume(cur, obj_id)
                else:
                    # always delete both header & body
                    self._delete(cur, obj_id)
        finally:
            the_pool.putconn(conn)

    def _serve_root(self):
        inst_name = get_option("instance", "")
        config = """[root]
instance=%s
""" % (inst_name,)

        self.send_response(200)
        self.send_header('Content-type', "text/plain; encoding=utf-8")
        self.end_headers()
        self.wfile.write(config.encode("utf-8"))
        # self.close_connection = False

    def _serve(self, cur, url_id, headers_flag):
        compress_threshold = int(get_option("compress_threshold", "100"))
        bridge = StorageBridge(cur)
        try:
            if not bridge.has_local_data(url_id):
                self.send_error(404, "File not found")
                return

            volume_id = bridge.get_volume_id(url_id)
            if headers_flag:
                sz = None
                ct = "text/plain"
                reader = bridge.open_headers(url_id, volume_id)
            else:
                sz = bridge.get_body_size(url_id, volume_id)
                ct = bridge.get_content_type(url_id, volume_id)
                reader = bridge.open_page(url_id, volume_id)

            if reader is None:
                # headers are optional (e.g. drive.py doesn't store
                # them); for bodies, assuming redirect
                self.send_error(204, "No content")
                return

            try:
                compress = (sz is not None) and (sz >= compress_threshold) and ('accept-encoding' in self.headers) and ('gzip' in self.headers['accept-encoding'])

                self.send_response(200)
                self.send_header('Content-Type', ct)

                if compress:
                    self.send_header('Content-Encoding', 'gzip')
                elif sz is not None:
                    self.send_header('Content-Length', sz)

                self.end_headers()

                if compress:
                    self._write_compressed(reader)
                else:
                    shutil.copyfileobj(reader, self.wfile)

                # self.close_connection = False
            finally:
                reader.close()
        finally:
            bridge.close()

    def _serve_volume(self, cur, volume_id):
        bridge = VolumeBridge(cur)
        if not bridge.has_local_volume(volume_id):
            self.send_error(404, "Archive not found")
            return

        reader = bridge.open_volume(volume_id)
        if not reader:
            self.send_error(404, "Volume not found")
            return

        try:
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.end_headers()
            shutil.copyfileobj(reader, self.wfile)
        finally:
            reader.close()

    def _delete(self, cur, url_id):
        bridge = StorageBridge(cur)
        try:
            if not bridge.has_remote_instance(url_id):
                self.send_error(403, "Forbidden")
                return

            volume_id = bridge.get_volume_id(url_id)
            if volume_id:
                self.send_error(403, "Already archived")
                return

            bridge.delete_storage(url_id)
            self.send_error(204, "No content")
        finally:
            bridge.close()

    def _delete_volume(self, cur, volume_id):
        bridge = VolumeBridge(cur)
        if not bridge.has_remote_instance(volume_id):
            self.send_error(403, "Forbidden")
            return

        bridge.delete_volume(volume_id)
        self.send_error(204, "No content")

    def _write_compressed(self, reader):
        compressor = gzip.GzipFile(fileobj=self.wfile, mode='w', compresslevel=5)
        try:
            shutil.copyfileobj(reader, compressor)
        finally:
            compressor.close()


def main():
    try:
        raw_port = get_option('server_port', "8888")
        server = ThreadingHTTPServer(('', int(raw_port)), StorageHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        print("shutting down on keyboard interrupt", file=sys.stderr)
        server.socket.close()
        the_pool.closeall()

if __name__ == "__main__":
    main()
