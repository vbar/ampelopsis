#!/usr/bin/python3

from datetime import datetime
from io import BytesIO
import os
import pycurl
import re
import sys
from urllib.parse import urlparse, urlunparse
from act_util import act_inc, act_dec
from common import get_loose_path, get_netloc, get_option, make_connection
from download_base import DownloadBase

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
        self.retry_after = None

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
            elif name == 'retry-after':
                self.retry_after = value
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
            if os.path.exists(old_path):
                if self.retrieve_body:
                    os.rename(old_path, get_loose_path(self.eff_id))
                else:
                    os.remove(old_path)

        self.owner.finish_page(self.url_id, self.eff_id, self.retrieve_body)


class Retriever(DownloadBase):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, conn, cur, single_action)

        self.max_num_conn = int(get_option('max_num_conn', "10"))
        self.notification_threshold = int(get_option('download_notification_threshold', "1000"))
        self.force_ipv6 = get_option('force_ipv6', None)
        self.user_agent = get_option('user_agent', None)
        self.extra_header = get_option('extra_header', None)
        self.socks_proxy_host = get_option('socks_proxy_host', None)
        self.socks_proxy_port = int(get_option('socks_proxy_port', "0"))

        retry_after_default = get_option('retry_after_default', None)
        self.retry_after_default = None if retry_after_default is None else int(retry_after_default)

        self.mime_whitelist = { 'text/html' }
        mime_whitelist = get_option('mime_whitelist', None)
        if mime_whitelist:
            if mime_whitelist == "*":
                self.mime_whitelist = set()
            else:
                self.mime_whitelist.update(mime_whitelist.split())

    def is_acceptable(self, content_type):
        if len(self.mime_whitelist) == 0:
            return True

        lst = content_type.split(';', 2)
        mime_type = lst[0]
        return mime_type.lower() in self.mime_whitelist

    def retrieve_all(self):
        while self.retrieve():
            pass

    # adapted from https://github.com/pycurl/pycurl/blob/master/examples/retriever-multi.py
    def retrieve(self):
        avail = self.get_available_hosts()
        if not len(avail):
            return False

        self.cur.execute("""select count(*)
from download_queue
where host_id = any(%s)""", (sorted(avail),))
        row = self.cur.fetchone()
        num_conn = row[0]
        if not num_conn:
            return False

        full = num_conn >= self.max_num_conn
        if full:
            num_conn = self.max_num_conn

        m = pycurl.CurlMulti()
        m.handles = []
        for i in range(num_conn):
            c = pycurl.Curl()
            c.setopt(pycurl.FOLLOWLOCATION, 1)
            c.setopt(pycurl.MAXREDIRS, 5)
            c.setopt(pycurl.CONNECTTIMEOUT, 30)
            c.setopt(pycurl.TIMEOUT, 300)

            if self.force_ipv6:
                c.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_V6)

            if self.user_agent:
                c.setopt(pycurl.USERAGENT, self.user_agent)

            if self.extra_header:
                c.setopt(pycurl.HTTPHEADER, [self.extra_header])

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
                    eff_hostname = None
                    if target.url != eff_url:
                        pr = urlparse(eff_url)
                        eff_hostname = pr.hostname
                        clean_pr = (pr.scheme, get_netloc(pr), pr.path, pr.params, pr.query, '')
                        clean_url = urlunparse(clean_pr)
                        if target.url != clean_url:
                            eff_id, known = self.add_redirect(target.url_id, clean_url)
                            target.eff_id = eff_id
                            if known:
                                target.retrieve_body = False

                    msg = "got " + eff_url
                    added_hold = False
                    if target.http_code != 200:
                        self.cur.execute("""insert into download_error(url_id, error_code, error_message, failed)
values(%s, %s, %s, localtimestamp)""", (target.url_id, target.http_code, target.http_phrase))

                        # HTTP 429 response need not include
                        # Retry-After (apparently it depends on the
                        # server) while Retry-After is specified for a
                        # couple of HTTP error codes
                        if (target.retry_after is not None) or ((target.http_code == 429) and (self.retry_after_default is not None)):
                            if eff_hostname is None:
                                pr = urlparse(target.url)
                                eff_hostname = pr.hostname

                            if eff_hostname is None:
                                print("cannot parse " + target.url, file=sys.stderr)
                            else:
                                retry_after = self.retry_after_default if target.retry_after is None else target.retry_after
                                added_hold = self.add_hold(eff_hostname, retry_after)

                        if target.http_code is None:
                            msg += " with no HTTP status"
                        else:
                            msg += " with %d" % target.http_code

                    print(msg, file=sys.stderr)
                    if added_hold:
                        print("added hold on " + eff_hostname, file=sys.stderr)

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
            if hasattr(c, 'target') and (c.target is not None):
                c.target.close()
                c.target = None

            c.close()

        m.close()
        return full

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
                act_inc(cur)
                retriever.retrieve_all()
                global_live = act_dec(cur)
                if single_action:
                    break
                else:
                    future_live = retriever.cond_notify()
                    if global_live or future_live or retriever.has_holds():
                        retriever.wait()
                    else:
                        retriever.do_notify()
                        print("all done", file=sys.stderr)
                        break

if __name__ == "__main__":
    main()
