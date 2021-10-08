#!/usr/bin/python3

import os
import sys
from act_util import act_inc, act_dec
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from common import get_loose_path, get_option, get_parent_directory, make_connection
from download_base import DownloadBase

class Driver(DownloadBase):
    def __init__(self, single_action, conn, cur):
        DownloadBase.__init__(self, conn, cur, single_action)
        self.br = None
        self.notification_threshold = int(get_option('drive_notification_threshold', "500"))
        self.drive_headless = get_option('drive_headless', False)
        self.socks_proxy_host = get_option('socks_proxy_host', None)
        self.socks_proxy_port = int(get_option('socks_proxy_port', "0"))
        self.download_dir = os.path.join(get_parent_directory(), "down")
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def lazy_init(self):
        if self.br:
            return

        options = webdriver.ChromeOptions();

        if self.drive_headless:
            options.add_argument("--headless");
        else:
            options.add_argument("--start-maximized");

        if self.socks_proxy_host:
            proxy_url = "socks5://%s:%d" % (self.socks_proxy_host, self.socks_proxy_port)
            options.add_argument("--proxy-server=" + proxy_url);

        prefs = { "download.default_directory": self.download_dir }
        options.add_experimental_option("prefs", prefs)
        self.br = webdriver.Chrome(executable_path='chromedriver', options=options)

    def run(self):
        self.cur.execute("""select count(*)
from download_queue""")
        row = self.cur.fetchone()
        num_conn = row[0]
        if not num_conn:
            return

        self.lazy_init()

        batch_processed = 0
        row = self.pop_work_item()
        while row:
            url_id = row[0]
            url = self.get_url(url_id)
            self.br.get(url)
            error_code = None
            try:
                WebDriverWait(self.br, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'a')))
            except exceptions.TimeoutException:
                error_code = 500

            eff_id = url_id
            eff_url = self.br.current_url
            msg = "got " + eff_url
            if error_code:
                msg += " with %d" % error_code

            print(msg, file=sys.stderr)

            if url != eff_url:
                eff_id, known = self.add_redirect(url_id, eff_url)

            body = self.br.page_source
            with open(get_loose_path(url_id), 'w') as f:
                f.write(body)

            if error_code:
                self.cur.execute("""insert into download_error(url_id, error_code, failed)
values(%s, %s, localtimestamp)
on conflict(url_id) do update
set error_code=%s, failed=localtimestamp""", (url_id, error_code, error_code))

                self.br.close()
                self.br = None
                self.lazy_init()

            self.finish_page(url_id, eff_id, not error_code)

            batch_processed += 1
            if batch_processed >= self.notification_threshold:
                self.cond_notify()
                batch_processed = 0

            row = self.pop_work_item()

    def close(self):
        if self.br:
            self.br.close()


def main():
    single_action = (len(sys.argv) == 2) and (sys.argv[1] == '--single-action')

    conn = make_connection()
    try:
        with conn.cursor() as cur:
            driver = Driver(single_action, conn, cur)
            while True:
                act_inc(cur)
                driver.run()
                global_live = act_dec(cur)
                if single_action:
                    break
                else:
                    future_live = driver.cond_notify()
                    if global_live or future_live:
                        driver.wait()
                    else:
                        driver.last_notify()
                        driver.close()
                        print("all done")
                        break
    finally:
        conn.close()


if __name__ == "__main__":
    main()
