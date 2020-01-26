#!/usr/bin/python3

import sys
import json
import shutil
import common
from json_lookup import JsonLookup
from schema_manager import SchemaManager


class Recycler(JsonLookup):
    def __init__(self, old_schema, new_schema, cur, primary):
        JsonLookup.__init__(self, cur)
        self.process = self.process_primary if primary else self.process_secondary
        self.primary = primary
        self.old_schema = old_schema
        self.new_schema = new_schema
        self.input_count = 0
        self.output_count = 0

    def run(self):
        print("searching for recycleable URLs in %s..." % self.old_schema, file=sys.stderr)

        if self.primary:
            self.run_cycle("^https://cro.justice.cz/verejnost/api/funkcionari\\?order=DESC&page=[0-9]+&pageSize=[0-9]+&sort=created$")

        self.run_cycle("^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$")

    def run_cycle(self, url_mask):
        self.cur.execute("""select url
from field
left join download_error on id=url_id
where url ~ %s
        and checkd is not null and url_id is null
order by url""", (url_mask,))
        rows = self.cur.fetchall()
        for row in rows:
            self.process(row[0])

    def process_primary(self, detail_url):
        url_id = self.get_url_id(detail_url)
        if url_id is None:
            return None

        volume_id = self.get_volume_id(url_id)
        self.recycle(detail_url, url_id, volume_id)

        self.input_count += 1
        if not (self.input_count % 1000):
            print("recycled %d detail URLs..." % self.input_count)

    def process_secondary(self, detail_url):
        detail = self.get_document(detail_url)
        if not detail:
            # select in run should filter out all download errors, but
            # it's still theoretically possible to have parsing
            # errors...
            print(detail_url + " not found", file=sys.stderr)
        else:
            position_set = self.make_position_set(detail)
            if len(position_set):
                query_urls = self.make_query_urls(detail, position_set)
                for qurl in query_urls:
                    triple = self.check(qurl)
                    if triple and triple[0]:
                        self.recycle(qurl, triple[1], triple[2])

        self.input_count += 1
        if not (self.input_count % 1000):
            print("checked %d detail URLs, recycled %d queries..." %
                  (self.input_count, self.output_count), file=sys.stderr)

    def check(self, url):
        url_id = self.get_url_id(url)
        if url_id is None:
            return None

        volume_id = self.get_volume_id(url_id)
        reader = self.open_page(url_id, volume_id)
        if not reader:
            return None

        buf = b''
        try:
            for ln in reader:
                buf += ln
        finally:
            reader.close()

        doc = json.loads(buf.decode('utf-8'))
        bindings = doc['results']['bindings']
        return (len(bindings), url_id, volume_id)

    def recycle(self, url, old_url_id, old_volume_id):
        self.cur.execute("""select checkd
from field
where id=%s""", (old_url_id,))
        row = self.cur.fetchone()
        checkd = row[0]

        with SchemaManager(self.new_schema, self.cur):
            new_url_id = self.prepare(url)
            if new_url_id is None:
                return

            new_header_path = common.get_loose_path(new_url_id, True)
            new_body_path = common.get_loose_path(new_url_id)

        reader = self.open_headers(old_url_id, old_volume_id)
        if reader:
            try:
                with open(new_header_path, 'wb') as writer:
                    shutil.copyfileobj(reader, writer)
            finally:
                reader.close()

        with self.open_page(old_url_id, old_volume_id) as reader:
            with open(new_body_path, 'wb') as writer:
                shutil.copyfileobj(reader, writer)

        with SchemaManager(self.new_schema, self.cur):
            # generally a new page should go into parse_queue - but
            # queries have no further links
            parsed_date = None if self.primary else checkd
            self.cur.execute("""update field
set checkd=%s, parsed=%s
where id=%s""", (checkd, parsed_date, new_url_id))

            if parsed_date is None:
                self.cur.execute("""insert into parse_queue(url_id) values(%s)
on conflict(url_id) do nothing""", (new_url_id,))

        self.output_count += 1

    # the only Recycler method assuming new schema context
    def prepare(self, url):
        self.cur.execute("""select id, checkd, url_id
from field
left join download_error on id=url_id
where url=%s""", (url,))
        row = self.cur.fetchone()
        if row is None:
            self.cur.execute("""insert into field(url)
values(%s)
returning id""", (url,))
            row = self.cur.fetchone()
            return row[0]
        else:
            new_url_id = row[0]

            new_volume_id = self.get_volume_id(new_url_id)
            if new_volume_id is not None:
                print("cannot recycle %s into archived data" % (url,), file=sys.stderr)
                return None

            if row[1]:
                if row[2]:
                    print("replacing newly failed %s by old success" % (url,), file=sys.stderr)
                    self.cur.execute("""update field
set checkd=null
where id=%s""", (new_url_id,))
                    self.cur.execute("""delete from download_error
where url_id=%s""", (new_url_id,))
                else:
                    print("%s already in %s" % (url, self.new_schema), file=sys.stderr)
                    return None

            return new_url_id


def main():
    primary = common.get_option("recycle_primary", False)
    new_schema = common.schema
    with common.make_connection() as conn:
        with conn.cursor() as cur:
            old_schema = common.get_mandatory_option("old_schema")
            with SchemaManager(old_schema, cur):
                recycler = Recycler(old_schema, new_schema, cur, primary)
                recycler.run()

if __name__ == "__main__":
    main()
