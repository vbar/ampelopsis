#!/usr/bin/python3

from base64 import b64encode
import json
import os
import sys
from time import sleep
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from act_util import act_inc, act_dec
from common import get_loose_path, get_mandatory_option, get_option, make_connection
from download_base import DownloadBase
from leaf_load import LeafLoader

class Acquirer(DownloadBase):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, conn, cur, single_action)
        self.leaf_loader = LeafLoader(cur)
        self.server_url = get_option('oauth_server_url', "https://cro.justice.cz/verejnost/api/auth/basic")
        self.request_timeout = int(get_option('request_timeout', "60"))
        self.request_backoff = int(get_option('request_backoff', "300"))
        self.notification_threshold = int(get_option('download_notification_threshold', "1000"))

        user = get_mandatory_option('oauth_user')
        password = get_mandatory_option('oauth_password')
        src = user + ':' + password
        src_bytes = src.encode('ascii')
        dst_bytes = b64encode(src_bytes)
        self.basic_auth = dst_bytes.decode('ascii')
        self.token = None

    def run(self):
        self.cur.execute("""select count(*)
from download_queue""")
        row = self.cur.fetchone()
        num_conn = row[0]
        if not num_conn:
            return

        num_processed = 0
        row = self.pop_work_item()
        while row:
            self.acquire(row[0])

            num_processed += 1
            if num_processed >= self.notification_threshold:
                self.cond_notify()
                num_processed = 0

            row = self.pop_work_item()

    def acquire(self, person_url_id):
        person_url = self.get_url(person_url_id)
        m = self.leaf_loader.person_url_rx.match(person_url)
        if not m:
            print("unexpected URL " + person_url, file=sys.stderr)
            return

        person_id = m.group('id')

        parsed_urls = set()

        doc = self.leaf_loader.get_old_doc(person_url_id)
        if not doc:
            person_body = self.retrieve(person_url, person_url_id, person_url_id)
            if person_body:
                doc = self.safe_parse(person_body, person_url_id)

        if self.leaf_loader.merge_leaves and doc:
            statements = doc.get('statements')
            if isinstance(statements, list):
                first = True
                for statement in statements:
                    statement_id = statement.get('id')
                    if statement_id and self.leaf_loader.id_rx.match(statement_id):
                        if not self.leaf_loader.load_statement(person_id, statement_id):
                            if not len(parsed_urls):
                                gate_url_id = self.retrieve_gate(person_id, person_url_id)
                                if not gate_url_id:
                                    break

                                parsed_urls.add(gate_url_id)

                            statement_url_id = self.retrieve_statement(person_id, statement_id, person_url_id)
                            if not statement_url_id:
                                break

                            parsed_urls.add(statement_url_id)

        # parsing accesses statement URLs from person doc, and gate
        # URLs not at all - no need to queue them...
        if len(parsed_urls):
            parsed = ", ".join(( str(uid) for uid in sorted(parsed_urls) ))
            sql = """update field
set parsed=localtimestamp
where id in (%s)""" % parsed
            self.cur.execute(sql)

        if doc:
            self.finish_page(person_url_id, person_url_id, True)

    def authenticate(self):
        request = Request(self.server_url)
        request.add_header("Authorization", "Basic %s" % self.basic_auth)
        try:
            response = urlopen(request, timeout=self.request_timeout)
            if response.status != 200:
                msg = "%s got %s" % (self.server_url, response.status)
                self.report_auth_error(msg)
                return

            print("got " + self.server_url, file=sys.stderr)
            self.token = response.getheader('Bearer')
            if not self.token:
                raise Exception("no bearer token")
        except HTTPError as exc:
            msg = "%s failed with %d" % (self.server_url, exc.code)
            self.report_auth_error(msg)
        except OSError as exc:
            errno = exc.errno or 0
            msg += "%s failed: %d" %  (self.server_url, errno)
            self.report_auth_error(msg)

    def report_auth_error(self, msg):
        msg += " - will try again in %d seconds" % self.request_backoff
        print(msg, file=sys.stderr)
        sleep(self.request_backoff)

    def retrieve_gate(self, person_id, person_url_id):
        url = self.leaf_loader.make_gate_url(person_id)
        url_id = self.ensure_url_id(url)
        body = self.retrieve(url, url_id, person_url_id)
        if body is None:
            print("gate closed", file=sys.stderr)
            assert self.token is None
        else:
            answer = body.decode('utf-8')
            if 'false' in answer:
                self.token = None

        return url_id if self.token else None

    def retrieve_statement(self, person_id, statement_id, person_url_id):
        url = self.leaf_loader.make_statement_url(person_id, statement_id)
        url_id = self.ensure_url_id(url)
        self.retrieve(url, url_id, person_url_id)
        return url_id if self.token else None

    def retrieve(self, url, url_id, person_url_id):
        request = Request(url)

        while not self.token:
            self.authenticate()

        request.add_header("Authorization", "Bearer %s" % self.token)

        body = None
        msg = "got " + url
        try:
            response = urlopen(request, timeout=self.request_timeout)
            if response.status != 200:
                msg += " with %d" % response.status
                self.report_error(person_url_id, response.status, "can't do")

            # FIXME: should also save headers
            body = response.read()
        except HTTPError as exc:
            msg += " with %d" % exc.code
            self.report_error(person_url_id, exc.code, exc.reason)
        except OSError as exc:
            errno = exc.errno or 0
            msg += " with %d" % errno
            self.report_error(person_url_id, errno, exc.strerror)

        target_path = get_loose_path(url_id)
        if body:
            with open(target_path, 'wb') as f:
                f.write(body)
        else:
            if os.path.exists(target_path):
                os.remove(target_path)

        self.cur.execute("""update field
set checkd=localtimestamp, parsed=null
where id=%s""", (url_id,))

        print(msg, file=sys.stderr)
        return body if self.token else None

    def safe_parse(self, body, url_id):
        try:
            return json.loads(body.decode('utf-8'))
        except Exception as ex:
            msg = "%s: %s" % (type(ex), ex.msg if hasattr(ex, 'msg') else str(ex))
            self.cur.execute("""insert into parse_error(url_id, error_message, failed)
values(%s, %s, localtimestamp)
on conflict(url_id) do update
set error_message=%s, failed=localtimestamp""", (url_id, msg, msg))

            return None

    def ensure_url_id(self, url):
        # gate & statement URLs are always considered downloaded, so
        # that nobody tries to download them but this script
        self.cur.execute("""insert into field(url, checkd) values(%s, localtimestamp)
on conflict(url) do nothing
returning id""", (url,))
        row = self.cur.fetchone()
        if row is None:
            self.cur.execute("""select id from field
where url=%s""", (url,))
            row = self.cur.fetchone()

        # leaf URLs do not maintain locality - nobody needs it (except
        # health check, but if health check gets a too-pessimistic
        # view of download progress it makes no difference), the
        # simplest implementation (here) wouldn't be atomic WRT the
        # insert above and just enable them to leak into the download
        # queue...
        return row[0]

    def report_error(self, url_id, errno, errmsg):
        self.cur.execute("""insert into download_error(url_id, error_code, error_message, failed)
values(%s, %s, %s, localtimestamp)""", (url_id, errno, errmsg))
        self.token = None # abandon current detail and start again


def main():
    single_action = (len(sys.argv) == 2) and (sys.argv[1] == '--single-action')

    with make_connection() as conn:
        with conn.cursor() as cur:
            acquirer = Acquirer(single_action, conn, cur)
            while True:
                act_inc(cur)
                acquirer.run()
                global_live = act_dec(cur)
                if single_action:
                    break
                else:
                    future_live = acquirer.cond_notify()
                    if global_live or future_live:
                        acquirer.wait()
                    else:
                        acquirer.do_notify()
                        print("all done")
                        break

if __name__ == "__main__":
    main()
