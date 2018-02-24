#!/usr/bin/python3

from datetime import datetime
from io import BytesIO
import os
import pycurl
import re
import select
import sys
from urllib.parse import urlparse, urlunparse
from common import get_loose_path, get_netloc, get_option, make_connection
from download_base import DownloadBase

class Target:
    def __init__(self, owner, url, url_id):
        self.owner = owner
        self.url = url
        self.url_id = url_id
        self.header_target = open(get_loose_path(url_id, True), 'wb')
        self.body_target = None
        self.retrieve_body = True
        self.http_code = None
        self.http_phrase = None
        
    def write_header(self, data):
        if self.header_target.write(data) != len(data):
            raise Exception("write error")
        
        header_line = data.decode('iso-8859-1')
        line_list = header_line.split(':', 1)
        if len(line_list) == 2:            
            name, value = line_list
            name = name.strip()
            name = name.lower()
            value = value.strip()

            if (name == 'content-type') and not self.owner.is_acceptable(value):
                self.retrieve_body = False
        else:
            line_list = header_line.split()
            l = len(line_list)
            # maxsplit doesn't work w/ whitespace delimiter (Python 3.5.2)
            if l > 3:
                tail = " ".join(line_list[2:])
                line_list = line_list[:2]
                line_list.append(tail)
                l = 3
                
            if l >= 2:
                proto = line_list[0].lower()
                if proto.startswith('http/'):
                    self.http_code = int(line_list[1])
                    self.http_phrase = line_list[2] if l == 3 else None
                
    def write(self, data):
        if self.body_target is None:
            if self.retrieve_body:
                self.body_target = open(get_loose_path(self.url_id), 'wb')
            else:
                return -1
            
        return self.body_target.write(data)
    
    def close(self):
        self.header_target.close()
        self.header_target = None
        
        if self.body_target:
            self.body_target.close()
            self.body_target = None
            
        self.owner.finish_page(self.url_id)
        
    
class Retriever(DownloadBase):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, cur)
        
        self.single_action = single_action
        self.conn = conn
        self.max_num_conn = int(get_option('max_num_conn', "10"))
        self.notification_threshold = int(get_option('download_notification_threshold', "1000"))
        self.user_agent = get_option('user_agent', None)
        self.socks_proxy_host = get_option('socks_proxy_host', None)
        self.socks_proxy_port = int(get_option('socks_proxy_port', "0"))

        self.mime_whitelist = { 'text/html' }
        mime_whitelist = get_option('mime_whitelist', None)
        if mime_whitelist:
            self.mime_whitelist.update(mime_whitelist.split())

    def is_acceptable(self, content_type):
        lst = content_type.split(';', 2)
        mime_type = lst[0]
        return mime_type.lower() in self.mime_whitelist
    
    def finish_page(self, url_id):
        self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (url_id,))

        self.cur.execute("""insert into parse_queue(url_id)
values(%s)""", (url_id,))
        
    def insert_multiple(self, ida, idb):
        if ida < idb:
            id1 = ida
            id2 = idb
        else:
            id1 = idb
            id2 = ida
                    
        self.cur.execute("""insert into multiple(id1, id2) values(%s, %s)
on conflict do nothing""", (id1, id2))
        
    def add_known_url(self, url_id, new_url):
        known = False
        new_url_id = None
        self.cur.execute("""select id, checkd
from field
where url=%s""", (new_url,))
        row = self.cur.fetchone()
        if row is not None:
            new_url_id, checked = row
            known = checked is not None

        if new_url_id:
            self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (new_url_id,))
        else:
            # conflict probably won't happen, but theoretically it's
            # possible that a parallel download inserted the URL since
            # the select above...
            self.cur.execute("""insert into field(url, checkd) values(%s, localtimestamp)
on conflict(url) do update
set checkd=localtimestamp
returning id""", (new_url,))
            row = self.cur.fetchone()
            new_url_id = row[0]
            
        self.insert_multiple(url_id, new_url_id)
        return known
    
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
            
    # adapted from https://github.com/pycurl/pycurl/blob/master/examples/retriever-multi.py
    def retrieve(self):
        self.cur.execute("""select count(*)
from download_queue""")
        row = self.cur.fetchone()
        num_conn = row[0]
        if not num_conn:
            return False

        if num_conn > self.max_num_conn:
            num_conn = self.max_num_conn

        m = pycurl.CurlMulti()
        m.handles = []
        for i in range(num_conn):
            c = pycurl.Curl()
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.CONNECTTIMEOUT, 30)
            c.setopt(pycurl.TIMEOUT, 300)

            if self.user_agent:
                c.setopt(pycurl.USERAGENT, self.user_agent)

            if self.socks_proxy_host:
                c.setopt(pycurl.PROXY, self.socks_proxy_host)
                c.setopt(pycurl.PROXYPORT, self.socks_proxy_port)
                c.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5_HOSTNAME)
                
            m.handles.append(c)

        freelist = m.handles[:]
        num_started = 0
        num_processed = 0
        num_reported = 0
        while True:
            row = None
            if freelist:
                row = self.pop_work_item()
                
            while row:
                assert freelist
                
                url_id = row[0]
                url = self.get_url(url_id)
                assert url
                c = freelist.pop()
                num_started += 1
                c.target = Target(self, url, url_id)
                c.setopt(pycurl.URL, url)
                c.setopt(c.HEADERFUNCTION, c.target.write_header)
                c.setopt(pycurl.WRITEDATA, c.target)
                m.add_handle(c)
                
                if freelist:
                    row = self.pop_work_item()
                else:
                    row = None
                    
            while True:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break                

            while True:
                num_q, ok_list, err_list = m.info_read()
                for c in ok_list:
                    target = c.target
                    m.remove_handle(c)
                    eff_url = c.getinfo(pycurl.EFFECTIVE_URL)
                    if target.url != eff_url:
                        pr = urlparse(eff_url)
                        clean_pr = (pr.scheme, get_netloc(pr), pr.path, pr.params, pr.query, '')
                        clean_url = urlunparse(clean_pr)
                        if (target.url != clean_url) and self.add_known_url(target.url_id, clean_url):
                            target.body_buffer = None

                    msg = "got " + eff_url
                    if target.http_code != 200:
                        self.cur.execute("""insert into download_error(url_id, error_code, error_message, failed)
values(%s, %s, %s, localtimestamp)""", (target.url_id, target.http_code, target.http_phrase))
                        
                        if target.http_code is None:
                            msg += " with no HTTP status"
                        else:
                            msg += " with %d" % target.http_code
                        
                    print(msg, file=sys.stderr)
                    target.close()
                    c.target = None
                    freelist.append(c)
                    
                for c, errno, errmsg in err_list:
                    target = c.target
                    target.close()
                    c.target = None
                    m.remove_handle(c)
                    self.report_error(target, errno, errmsg)
                    freelist.append(c)

                num_processed += len(ok_list) + len(err_list)
                
                if num_q == 0:
                    break

            if (num_processed - num_reported) >= self.notification_threshold:
                self.cond_notify()
                num_reported = num_processed
                
            if num_started == num_processed:
                break

            m.select(1.0)
            
        for c in m.handles:
            if c.target is not None:
                c.target.close()
                c.target = None
                
            c.close()
                
        m.close()
        return True

    def report_error(self, target, errno, errmsg):
        if (errno == 23) and not target.retrieve_body:
            # cancelled on uninteresting Content-Type
            return
        
        print("Failed:", errno, errmsg, file=sys.stderr)
        self.cur.execute("""insert into download_error(url_id, error_code, error_message, failed)
values(%s, %s, %s, localtimestamp)""", (target.url_id, errno, errmsg))


def main():
    single_action = (len(sys.argv) == 2) and (sys.argv[1] == '--single-action')
    
    with make_connection() as conn:
        with conn.cursor() as cur:
            retriever = Retriever(single_action, conn, cur)
            while True:
                retriever.retrieve()
                if single_action:
                    break
                else:
                    retriever.cond_notify()
                    retriever.wait()
            
if __name__ == "__main__":
    main()
