#!/usr/bin/python3

from datetime import datetime
from io import BytesIO
import os
import pycurl
import re
import select
import shutil
import sys
from urllib.parse import urlparse, urlunparse
from common import get_loose_path, get_netloc, get_option, make_connection
from download_base import DownloadBase
from volume_holder import VolumeHolder

class Target:
    def __init__(self, owner, url, url_id):
        self.owner = owner
        self.url = url
        self.url_id = url_id
        self.eff_id = url_id
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
        if not header_line.startswith('HTTP/') and (len(line_list) == 2):
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

        if self.url_id != self.eff_id:
            os.rename(get_loose_path(self.url_id, True), get_loose_path(self.eff_id, True))

            old_path = get_loose_path(self.url_id)
            if self.retrieve_body:
                os.rename(old_path, get_loose_path(self.eff_id))
            elif os.path.exists(old_path):
                os.remove(old_path)

        self.owner.finish_page(self.url_id, self.eff_id, self.retrieve_body)


class Retriever(DownloadBase, VolumeHolder):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, cur)
        VolumeHolder.__init__(self)

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

    def finish_page(self, url_id, eff_id, has_body):
        self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (url_id,))

        if url_id != eff_id:
            self.cur.execute("""update field
set checkd=localtimestamp
where id=%s""", (eff_id,))

        if has_body:
            self.cur.execute("""insert into parse_queue(url_id)
values(%s)""", (eff_id,))

    def add_redirect(self, url_id, new_url):
        known = False
        new_url_id = None
        while new_url_id is None:
            self.cur.execute("""select id
from field
where url=%s""", (new_url,))
            row = self.cur.fetchone()
            if row is not None:
                new_url_id = row[0]
                known = True
            else:
                self.cur.execute("""insert into field(url) values(%s)
on conflict(url) do nothing
returning id""", (new_url,))
                row = self.cur.fetchone()
                # conflict probably won't happen, but theoretically
                # it's possible that a parallel download inserted the
                # URL since the select above, in which case we'll just
                # try again...
                if row is None:
                    print("parallel insert for " + new_url, file=sys.stderr)
                else:
                    new_url_id = row[0]

        self.cur.execute("""insert into redirect(from_id, to_id) values(%s, %s)
on conflict do nothing""", (url_id, new_url_id))

        return (new_url_id, known)

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

    def pop_work_item(self):
        row = super().pop_work_item()
        while row:
            url_id = row[0]
            url = self.get_url(url_id)

            self.cur.execute("""select id, checkd
from field_cache
where url = %s""", (url,))
            crow = self.cur.fetchone()
            if not crow:
                return row

            self.copy_cached(url, url_id, *crow)

            row = super().pop_work_item()

        return None

    def copy_cached(self, url, dst_id, src_id, checked):
        print("using cached " + url, file=sys.stderr)

        volume_id = self.get_volume_id(src_id)
        if volume_id is None:
            self.copy_cached_path(get_loose_path(src_id, True), get_loose_path(dst_id, True))
            self.copy_cached_path(get_loose_path(src_id), get_loose_path(dst_id))
        else:
            self.copy_cached_handle(self.open_headers(src_id, volume_id), get_loose_path(dst_id, True))
            self.copy_cached_handle(self.open_page(src_id, volume_id), get_loose_path(dst_id))

        self.cur.execute("""update field
set checkd=%s
where id=%s""", (checked, dst_id))

        self.cur.execute("""insert into parse_queue(url_id)
values(%s)""", (dst_id,))

    @staticmethod
    def copy_cached_path(src_path, dst_path):
        if os.path.exists(src_path):
            shutil.copyfile(src_path, dst_path)
        elif os.path.exists(dst_path):
            os.remove(path)

    @staticmethod
    def copy_cached_handle(src_handle, dst_path):
        if src_handle is not None:
            try:
                with open(dst_path, 'wb') as dst_handle:
                    shutil.copyfileobj(src_handle, dst_handle)
            finally:
                src_handle.close()
        elif os.path.exists(dst_path):
            os.remove(dst_path)

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
                        if target.url != clean_url:
                            eff_id, known = self.add_redirect(target.url_id, clean_url)
                            target.eff_id = eff_id
                            if known:
                                target.retrieve_body = False

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
