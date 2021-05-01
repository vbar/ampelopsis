#!/usr/bin/python3

from base64 import b64encode
import json
import os
import re
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from act_util import act_inc, act_dec
from common import get_loose_path, get_mandatory_option, get_option, make_connection
from download_base import DownloadBase

person_url_rx = re.compile("^https://cro.justice.cz/verejnost/api/funkcionari/(?P<id>[0-9a-fA-F-]{36})$")

id_rx = re.compile("^[0-9a-fA-F-]{36}$")

class Acquirer(DownloadBase):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, conn, cur, single_action)
        self.server_url = get_option('oauth_server_url', "https://cro.justice.cz/verejnost/api/auth/basic")

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

        row = self.pop_work_item()
        while row:
            self.acquire(row[0])
            row = self.pop_work_item()

    def acquire(self, person_url_id):
        if self.token is None:
            self.authenticate()

        person_url = self.get_url(person_url_id)
        m = person_url_rx.match(person_url)
        if not m:
            raise Exception("unexpected URL " + person_url)

        person_id = m.group('id')

        parsed_urls = { person_url_id }

        person_body = self.retrieve(person_url, person_url_id, person_url_id)
        if person_body:
            doc = self.safe_parse(person_body, person_url_id)
            if doc:
                statements = doc.get('statements')
                if isinstance(statements, list):
                    first = True
                    for statement in statements:
                        statement_id = statement.get('id')
                        if statement_id and id_rx.match(statement_id):
                            if len(parsed_urls) == 1:
                                gate_url_id = self.retrieve_gate(person_id, person_url_id)
                                if not gate_url_id:
                                    break

                                parsed_urls.add(gate_url_id)

                            statement_url_id = self.retrieve_statement(person_id, statement_id, person_url_id)
                            if not statement_url_id:
                                break

                            parsed_urls.add(statement_url_id)

        # until parser uses jumper, it doesn't have anything to do
        # with these URLs...
        parsed = ", ".join(( str(uid) for uid in sorted(parsed_urls) ))
        sql = """update field
set parsed=localtimestamp
where id in (%s)""" % parsed
        self.cur.execute(sql)

    def authenticate(self):
        request = Request(self.server_url)
        request.add_header("Authorization", "Basic %s" % self.basic_auth)
        response = urlopen(request)
        if response.status != 200:
            raise Exception("%s got %s" % (self.server_url, response.status))

        print("got " + self.server_url, file=sys.stderr)
        self.token = response.getheader('Bearer')
        if not self.token:
            raise Exception("no bearer token")

    def retrieve_gate(self, person_id, person_url_id):
        url = "https://cro.justice.cz/verejnost/api/funkcionari/schvaleno/" + person_id
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
        url = "https://cro.justice.cz/verejnost/api/funkcionari/%s/oznameni/%s" % (person_id, statement_id)
        url_id = self.ensure_url_id(url)
        self.retrieve(url, url_id, person_url_id)
        return url_id if self.token else None

    def retrieve(self, url, url_id, person_url_id):
        assert self.token

        request = Request(url)
        request.add_header("Authorization", "Bearer %s" % self.token)
        body = None
        msg = "got " + url
        try:
            response = urlopen(request)
            if response.status != 200:
                msg += " with %d" % response.status
                self.report_error(person_url_id, response.status, "can't do")

            # FIXME: should also save headers
            body = response.read()
        except HTTPError as exc:
            msg += " with %d" % exc.code
            self.report_error(person_url_id, exc.code, exc.reason)

        target_path = get_loose_path(url_id)
        if body:
            with open(target_path, 'wb') as f:
                f.write(body)
        else:
            if os.path.exists(target_path):
                os.remove(target_path)

        self.cur.execute("""update field
set checkd=localtimestamp
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
        self.cur.execute("""insert into field(url) values(%s)
on conflict(url) do nothing
returning id""", (url,))
        row = self.cur.fetchone()
        if row is None:
            self.cur.execute("""select id from field
where url=%s""", (url,))
            row = self.cur.fetchone()

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
