#!/usr/bin/python3

from datetime import datetime
from io import BytesIO
import os
import pycurl
import re
import select
import sys
import zipfile
from common import get_option, get_volume_path, make_connection

class Target:
    def __init__(self, owner, url, url_id, member_id):
        self.owner = owner
        self.url = url
        self.url_id = url_id
        self.member_id = member_id
        self.header_buffer = BytesIO()
        self.body_buffer = None
        self.retrieve_body = True
        
    def write_header(self, data):
        self.header_buffer.write(data)
        
        header_line = data.decode('iso-8859-1')
        line_list = header_line.split(':', 1)
        if len(line_list) != 2:
            return

        name, value = line_list
        name = name.strip()
        name = name.lower()
        value = value.strip()

        if (name == 'content-type') and not value.startswith('text/html'):
            self.retrieve_body = False
            
    def write(self, data):
        if self.body_buffer is None:
            if self.retrieve_body:
                self.body_buffer = BytesIO()
            else:
                return -1
            
        self.body_buffer.write(data)
        return None
    
    @staticmethod
    def make_info(buf, name, dt):
        if buf is None:
            return None
        
        sz = buf.tell()
        buf.seek(0)
        info = zipfile.ZipInfo(name, dt)
        info.file_size = sz
        info.compress_type = zipfile.ZIP_DEFLATED

        # https://stackoverflow.com/questions/434641/how-do-i-set-permissions-attributes-on-a-file-in-a-zip-file-using-pythons-zip
        info.external_attr = 0o666 << 16 # give read+write access to included file
        
        return info
    
    def close(self):
        stem = str(self.member_id)
        dtime = datetime.now()
        dt = (dtime.year, dtime.month, dtime.day, dtime.hour, dtime.minute, dtime.second)
        header_info = Target.make_info(self.header_buffer, stem + 'h', dt)
        body_info = Target.make_info(self.body_buffer, stem, dt)
        self.owner.write_member(self, header_info, body_info)
        self.header_buffer = None
        self.body_buffer = None
    
class Retriever:
    def __init__(self, single_action, conn, cur):
        self.single_action = single_action
        self.conn = conn
        self.cur = cur
        self.volume_id = None
        self.zip_front = None
        self.zip_back = None
        self.max_num_conn = int(get_option('max_num_conn', "10"))
        self.volume_threshold = int(get_option('volume_threshold', str(1024 * 1024 * 1024)))
                                               
    def pop_work_item(self):
        # https://blog.2ndquadrant.com/what-is-select-skip-locked-for-in-postgresql-9-5/
        self.cur.execute("""delete from download_queue
where url_id = (
        select url_id
        from download_queue
        order by priority, url_id
        for update skip locked
        limit 1
)
returning url_id""")
        return self.cur.fetchone()

    def get_url(self, url_id):
        self.cur.execute("""select url
from field
where id=%s""", (url_id,))
        return self.cur.fetchone()
        
    def set_checked(self, url_id):
        self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (url_id,))

    def write_member(self, target, header_info, body_info):
        if self.zip_front is None:
            self.volume_id = self.add_volume()
            archive_path = get_volume_path(self.volume_id)
            self.zip_back = open(archive_path, mode='wb')
            self.zip_front = zipfile.ZipFile(self.zip_back, mode='w', compression=zipfile.ZIP_DEFLATED)

        self.write_member_half(header_info, target.header_buffer)
        
        if body_info:
            self.write_member_half(body_info, target.body_buffer)
            
        self.cur.execute("""insert into content(url_id, volume_id, member_id)
values(%s, %s, %s)""", (target.url_id, self.volume_id, target.member_id))

    def write_member_half(self, info, buf):
        self.zip_front.writestr(info, buf.read())
        
    def insert_multiple(self, ida, idb):
        if ida < idb:
            id1 = ida
            id2 = idb
        else:
            id1 = idb
            id2 = ida
                    
        self.cur.execute("""insert into multiple(id1, id2) values(%s, %s)
on conflict do nothing""", (id1, id2))
        
    def add_volume(self):
        self.cur.execute("""insert into directory
default values
returning id""")
        row = self.cur.fetchone()
        return row[0]

    def close(self):
        if self.zip_front is not None:
            self.zip_front.close()
            self.zip_front = None
            self.zip_back.close()
            self.zip_back = None

    def is_full(self):
        if self.zip_back is None:
            return False
        
        statinfo = os.fstat(self.zip_back.fileno())
        sz = statinfo.st_size
        return sz > self.volume_threshold
            
    def finish_volume(self):
        if self.volume_id is None:
            return
        
        self.cur.execute("""update directory
set written=localtimestamp
where id=%s""", (self.volume_id,))
        
        self.cur.execute("""insert into parse_queue(url_id)
select url_id
from content
join field on url_id=id
where volume_id=%s
order by url_id""", (self.volume_id,))

        if not self.single_action:
            self.cur.execute("""notify parse_ready""")
        
        self.volume_id = None

    def add_known_url(self, old_url_id, new_url):
        self.cur.execute("""select count(*)
from content
where url_id=%s""", (old_url_id,))
        row = self.cur.fetchone()
        known = row[0] > 0
        
        self.cur.execute("""insert into field(url, checkd) values(%s, localtimestamp)
on conflict(url) do update
set checkd=localtimestamp
returning id""", (new_url,))
        row = self.cur.fetchone()
        self.insert_multiple(old_url_id, row[0])
        return known
    
    def retrieve_all(self):
        keep = True
        while keep:
            keep = self.retrieve()
            if self.is_full():
                self.close()
                self.finish_volume()

        self.close()
        self.finish_volume()
                
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
            m.handles.append(c)

        freelist = m.handles[:]
        num_started = 0
        num_processed = 0
        while True:
            row = None
            if freelist and not self.is_full():
                row = self.pop_work_item()
                
            while row:
                assert freelist
                
                url_id = row[0]
                subrow = self.get_url(url_id)
                url = subrow[0]
                self.set_checked(url_id)
                c = freelist.pop()
                num_started += 1
                c.target = Target(self, url, url_id, num_started)
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
                        if self.add_known_url(target.url_id, eff_url):
                            target.body_buffer = None
                        
                    print("downloaded " + eff_url, file=sys.stderr)
                    target.close()
                    c.target = None
                    freelist.append(c)
                    
                for c, errno, errmsg in err_list:
                    target = c.target
                    target.close()
                    c.target = None
                    m.remove_handle(c)

                    if (errno != 23) or target.retrieve_body:
                        print("Failed:", errno, errmsg, file=sys.stderr)
                        
                    freelist.append(c)

                num_processed += len(ok_list) + len(err_list)
                
                if num_q == 0:
                    break

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
        
def main():
    single_action = (len(sys.argv) == 2) and (sys.argv[1] == '--single-action')
    
    with make_connection() as conn:
        with conn.cursor() as cur:
            retriever = Retriever(single_action, conn, cur)
            try:
                while True:
                    retriever.retrieve_all()
                    if single_action:
                        break
                    else:
                        retriever.wait()
            finally:
                retriever.close()
            
if __name__ == "__main__":
    main()
