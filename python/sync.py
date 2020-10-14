#!/usr/bin/python3

import io
import pycurl
import re
import sys
from common import get_loose_path, get_mandatory_option, get_option, make_connection, schema
from cursor_wrapper import CursorWrapper
from host_check import get_instance_id

class TargetBase:
    def __init__(self, owner, url):
        self.owner = owner
        self.url = url
        self.target = None
        self.retrieve_body = True
        self.http_code = None
        self.http_phrase = None

    def get_verb(self):
        return 'GET'

    def succeeded(self):
        return self.http_code and ((self.http_code // 100) == 2)

    def handle_header(self, data):
        header_line = data.decode('iso-8859-1')
        if header_line.startswith('HTTP/'):
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
                if self.http_code == 204:
                    self.retrieve_body = False

                self.http_phrase = line_list[2] if l == 3 else None

    def write(self, data):
        if self.target is None:
            if self.retrieve_body:
                self.target = self.make_target()
            else:
                return -1

        return self.target.write(data)


class RootTarget(TargetBase):
    def __init__(self, owner, url):
        TargetBase.__init__(self, owner, url)
        self.inst_rx = re.compile("instance\s*=(.+)")

    def make_target(self):
        return io.BytesIO()

    def close(self):
        if self.http_code != 200:
            raise Exception("Cannot get remote config: %s" % self.http_code)

        b = None
        if self.target:
            b = self.target.getvalue()
            self.target = None

        if not b:
            raise Exception("root has no body")

        s = b.decode('utf-8')
        m = self.inst_rx.search(s)
        if not m:
            raise Exception("no instance in " + s)

        remote_inst = m.group(1)
        self.owner.set_remote_instance(remote_inst.strip())


class HeaderTarget(TargetBase):
    def __init__(self, owner, url, url_id):
        TargetBase.__init__(self, owner, url)
        self.url_id = url_id

    def make_target(self):
        return open(get_loose_path(self.url_id, True), 'wb')

    def close(self):
        if self.target:
            self.target.close()
            self.target = None

        self.owner.continue_page(self.url_id, self.succeeded())


class BodyTarget(TargetBase):
    def __init__(self, owner, url, url_id):
        TargetBase.__init__(self, owner, url)
        self.url_id = url_id

    def make_target(self):
        return open(get_loose_path(self.url_id), 'wb')

    def close(self):
        if self.target:
            self.target.close()
            self.target = None

        self.owner.finish_page(self.url_id, self.succeeded())


class DeleteTarget(TargetBase):
    def __init__(self, owner, url, url_id):
        TargetBase.__init__(self, owner, url)
        self.url_id = url_id

    def get_verb(self):
        return 'DELETE'

    def make_target(self):
        # technically we could ignore the data, but our server isn't
        # supposed to send any...
        raise Exception("got DELETE response w/ body")

    def close(self):
        assert self.target is None


class Retriever(CursorWrapper):
    def __init__(self, cur, inst_name=None):
        CursorWrapper.__init__(self, cur)
        self.own_max_num_conn = int(get_option('own_max_num_conn', "4"))
        self.healthcheck_interval = int(get_option("healthcheck_interval", "100"))
        self.healthcheck_threshold = int(get_option("healthcheck_threshold", "80"))
        if self.healthcheck_threshold <= 0:
            # disabled
            self.healthcheck_interval = 0

        self.target_queue = [] # of TargetBase descendants
        self.progressing = [] # of URL IDs
        self.total_checked = 0
        self.total_processed = 0
        self.total_error = 0

        server_name = get_mandatory_option('server_name')
        raw_port = get_option('server_port', "8888")
        self.endpoint_root = "http://%s:%d" % (server_name, int(raw_port))

        inst_name = get_option("instance", None)
        self.inst_id = get_instance_id(cur, inst_name)
        self.remote_inst_id = None # None => not initialized, "" => remote doesn't have instance

    def retrieve_all(self):
        while self.retrieve():
            pass

    # adapted from https://github.com/pycurl/pycurl/blob/master/examples/retriever-multi.py
    def retrieve(self):
        if self.remote_inst_id is None:
            self.target_queue.append(RootTarget(self, self.endpoint_root))
            num_conn = 1
            full = True
        else:
            if not self.remote_inst_id:
                self.cur.execute("""select count(*)
from field
left join download_error on id=download_error.url_id
left join locality on id=locality.url_id
where checkd is not null and failed is null and instance_id is null""")
            else:
                self.cur.execute("""select count(*)
from field
left join download_error on id=download_error.url_id
join locality on id=locality.url_id
where checkd is not null and failed is null and instance_id=%s""", (self.remote_inst_id,))
            row = self.cur.fetchone()
            num_conn = row[0]
            if not num_conn:
                return False

            full = num_conn >= self.own_max_num_conn
            if full:
                num_conn = self.own_max_num_conn

        m = pycurl.CurlMulti()
        m.handles = []
        for i in range(num_conn):
            c = pycurl.Curl()
            c.setopt(pycurl.CONNECTTIMEOUT, 30)
            c.setopt(pycurl.TIMEOUT, 300)
            m.handles.append(c)

        freelist = m.handles[:]
        num_started = 0
        num_processed = 0
        while True:
            target = None
            if freelist:
                target = self.pop_target()

            while target:
                assert freelist

                c = freelist.pop()
                num_started += 1
                if target.get_verb() == 'DELETE':
                    c.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
                else: # reset after potential DELETE
                    c.unsetopt(pycurl.CUSTOMREQUEST)
                    c.setopt(pycurl.HTTPGET, True)

                c.setopt(pycurl.URL, target.url)
                c.target = target
                c.setopt(c.HEADERFUNCTION, target.handle_header)
                c.setopt(pycurl.WRITEDATA, target)
                m.add_handle(c)

                if freelist:
                    target = self.pop_target()
                else:
                    target = None

            while True:
                ret, num_handles = m.perform()
                if ret != pycurl.E_CALL_MULTI_PERFORM:
                    break

            while True:
                num_q, ok_list, err_list = m.info_read()
                for c in ok_list:
                    target = c.target
                    m.remove_handle(c)
                    msg_verb = "deleted" if target.get_verb() == 'DELETE' else "got"
                    eff_url = c.getinfo(pycurl.EFFECTIVE_URL)
                    msg = msg_verb + " " + eff_url
                    if not target.succeeded():
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

                if self.healthcheck_interval and (self.total_processed >= self.total_checked + self.healthcheck_interval):
                    self.healthcheck()

                if num_q == 0:
                    break

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

    def get_url(self, url_id, headers_flag):
        if schema:
            url = "%s/%s/%d" % (self.endpoint_root, schema, url_id)
        else:
            url = "%s/%d" % (self.endpoint_root, url_id)

        if headers_flag:
            url += 'h'

        return url

    def pop_target(self):
        if len(self.target_queue):
            return self.target_queue.pop(0)

        cond_sql = ""
        if len(self.progressing):
            neg = ", ".join([ str(uid) for uid in self.progressing ])
            cond_sql = " and id not in (%s)" % neg

        if not self.remote_inst_id:
            self.cur.execute("""select id
from field
left join download_error on id=download_error.url_id
left join locality on id=locality.url_id
where checkd is not null and failed is null and instance_id is null%s
order by id
limit 1""" % cond_sql)
        else:
            self.cur.execute("""select id
from field
left join download_error on id=download_error.url_id
join locality on id=locality.url_id
where checkd is not null and failed is null%s and instance_id=%s
order by id
limit 1""" % (cond_sql, self.remote_inst_id))
        row = self.cur.fetchone()
        if not row:
            return None

        url_id = row[0]
        url = self.get_url(url_id, True)
        self.progressing.append(url_id)
        return HeaderTarget(self, url, url_id)

    def set_remote_instance(self, remote_inst):
        self.remote_inst_id = get_instance_id(self.cur, remote_inst) if remote_inst else ""
        if self.inst_id:
            if self.inst_id == self.remote_inst_id:
                raise Exception("Remote instance same as local")
        elif not self.remote_inst_id:
            raise Exception("Neither local nor remote instance is set")

    def continue_page(self, url_id, succeeded):
        if succeeded: # we could ignore the error, but then the body
                      # download might succeed, leading to silent loss
                      # of headers in the process of changing
                      # locality...
            url = self.get_url(url_id, False)
            self.target_queue.append(BodyTarget(self, url, url_id))
        else:
            self.total_error += 1

        self.total_processed += 1

    def finish_page(self, url_id, succeeded):
        if succeeded:
            if self.inst_id:
                self.cur.execute("""insert into locality(url_id, instance_id)
values(%s, %s)
on conflict(url_id) do update
set instance_id=%s""", (url_id, self.inst_id, self.inst_id))
            else:
                self.cur.execute("""delete from locality
where url_id=%s""", (url_id,))

            url = self.get_url(url_id, False)
            self.target_queue.append(DeleteTarget(self, url, url_id))
        else:
            self.total_error += 1

        self.total_processed += 1
        self.progressing.remove(url_id)

    def healthcheck(self):
        err_perc = (100 * self.total_error) / self.total_processed
        if err_perc >= self.healthcheck_threshold:
            msg = "on %d downloads, failed on %d" % (self.total_processed, self.total_error)
            raise Exception(msg)

        self.total_checked = self.total_processed

    def report_error(self, target, errno, errmsg):
        if (errno == 23) and not target.retrieve_body:
            return

        print("Failed:", errno, errmsg, file=sys.stderr)


def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            retriever = Retriever(cur)
            retriever.retrieve_all()

if __name__ == "__main__":
    main()
