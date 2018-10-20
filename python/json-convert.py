#!/usr/bin/python3

import json
import os
import sys
from common import get_parent_directory, make_connection
from json_lookup import JsonLookup

class Converter(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)

        self.doc_id = None
        self.url = None

        self.json_dir = os.path.join(get_parent_directory(), "json")
        if not os.path.exists(self.json_dir):
            os.makedirs(self.json_dir)

    def convert(self, url, url_id):
        print("converting " + url + "...", file=sys.stderr)

        volume_id = self.get_volume_id(url_id)
        buf = b""
        reader = self.open_page(url_id, volume_id)
        if not reader:
            print("not found", file=sys.stderr)
            return

        try:
            for ln in reader:
                buf += ln
        finally:
            reader.close()

        self.doc_id = None
        self.url = url
        doc = self.convert_node(json.loads(buf.decode('utf-8')), True)
        if not self.doc_id:
            raise Exception("page of %d has no Id" % url_id)

        target = os.path.join(self.json_dir, self.doc_id + ".json")
        with open(target, 'w') as writer:
            json.dump(doc, writer, ensure_ascii=False)

    def convert_node(self, in_node, top_level):
        if type(in_node) is dict:
            out_node = {}
            for k, v in in_node.items():
                if k == 'id':
                    k = 'Id'
                    if top_level:
                        self.doc_id = v

                out_node[k] = self.convert_node(v, False)
            if top_level:
                out_node['Url'] = self.url
                birthDate = self.get_birth_date(in_node['firstName'], in_node['lastName'])
                if birthDate:
                    out_node['birthDate'] = birthDate
        elif type(in_node) is list:
            out_node = []
            for it in in_node:
                out_node.append(self.convert_node(it, False))
        else:
            out_node = in_node

        return out_node

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            converter = Converter(cur)
            cur.execute("""select url, id
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$'""")
            rows = cur.fetchall()
            for row in rows:
                converter.convert(*row)

if __name__ == "__main__":
    main()
