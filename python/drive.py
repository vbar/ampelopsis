#!/usr/bin/python3

import os
import subprocess
import select
import socket
import sys
from urllib.parse import urlencode, parse_qsl
from urllib.request import urlopen
from common import get_loose_path, get_option, get_parent_directory, make_connection
from download_base import DownloadBase

class Acquirer(DownloadBase):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, cur)

        self.single_action = single_action
        self.conn = conn
        self.driver_timeout = int(get_option('driver_timeout', "60"))
        self.socks_proxy_host = get_option('socks_proxy_host', None)
        self.socks_proxy_port = int(get_option('socks_proxy_port', "0"))

        phantomjs_port = get_option('phantomjs_port', None)
        self.phantomjs_port = int(phantomjs_port) if phantomjs_port else None

        if self.phantomjs_port:
            self.server_url = 'http://localhost:%d' % (self.phantomjs_port,)
            self.acquire_one = self.acquire_ext
        else:
            js_dir = os.path.join(get_parent_directory(), "js")
            self.acquire_js = os.path.join(js_dir, "acquire.js")
            self.acquire_one = self.acquire_int
        
    def finish_page(self, url_id):
        self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (url_id,))

        self.cur.execute("""insert into parse_queue(url_id)
values(%s)""", (url_id,))
        
    def cond_notify(self):
        if not self.single_action:
            self.cur.execute("""select count(*)
from parse_queue""")
            row = self.cur.fetchone()
            if row[0] > 0:
                self.cur.execute("""notify parse_ready""")
                
    def wait(self):
        self.cur.execute("""LISTEN download_ready""")
        print("waiting for notification...")
        select.select([self.conn], [], [])
        self.conn.poll()
        print("got %d notification(s)" % (len(self.conn.notifies),))
        while self.conn.notifies:
            self.conn.notifies.pop()
            
    def acquire(self):
        row = self.pop_work_item()
        while row:
            url_id = row[0]
            url = self.get_url(url_id)
            assert url
            path = get_loose_path(url_id)
            assert path
            errpair = self.acquire_one(url, path)
            if not errpair:
                self.finish_page(url_id)
            else:
                print("Failed:", *errpair, file=sys.stderr)
                self.cur.execute("""insert into download_error(url_id, error_code, error_message, failed)
values(%s, %s, %s, localtimestamp)""", (url_id, *errpair))

            row = self.pop_work_item()
            
    def acquire_int(self, url, path):
        args = [ 'phantomjs' ]
        if self.socks_proxy_host:
            args.append("--proxy=%s:%d" % (self.socks_proxy_host, self.socks_proxy_port))
            args.append("--proxy-type=socks5")

        args.extend([ self.acquire_js, url, path ])
        
        print(*args, file=sys.stderr)
        completed = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=self.driver_timeout, check=True)
        return (completed.returncode, "") if completed.returncode else None
    
    def acquire_ext(self, url, path):
        cmd_dict = {'src': url, 'dst': path}
        data = urlencode(cmd_dict)
        try:
            rsp = urlopen(self.server_url, data.encode('utf-8'), self.driver_timeout)
        except socket.timeout as ex:
            errno = ex.errno if ex.errno else 110
            strerror = ex.strerror if ex.strerror else "timed out"
            return (errno, strerror)

        content = parse_qsl(rsp.read().decode('utf-8'))
        rsp.close()
        
        match = 0
        for pair in content:
            arg = cmd_dict.get(pair[0])
            if arg == pair[1]:
                match += 1

        if match == len(cmd_dict):
            print("saved " + url, file=sys.stderr)
            return None
        else:
            return (-1, content)

            
def main():
    single_action = (len(sys.argv) == 2) and (sys.argv[1] == '--single-action')
    
    with make_connection() as conn:
        with conn.cursor() as cur:
            acquirer = Acquirer(single_action, conn, cur)
            while True:
                acquirer.acquire()
                if single_action:
                    break
                else:
                    acquirer.cond_notify()
                    acquirer.wait()

if __name__ == "__main__":
    main()
            
