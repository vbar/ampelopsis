#!/usr/bin/python3

import json
import os
import sys
from common import get_parent_directory, make_connection
from json_lookup import JsonLookup

class Converter(JsonLookup):
    def __init__(self, cur):
        JsonLookup.__init__(self, cur)

        self.multiplicity = {}
        self.doc_id = None
        self.url = None

        self.json_dir = os.path.join(get_parent_directory(), "json")
        if not os.path.exists(self.json_dir):
            os.makedirs(self.json_dir)

    def run(self):
        self.cycle(True)
        self.cycle(False)

    def cycle(self, prep):
        self.cur.execute("""select url, id
from field
where url ~ '^https://cro.justice.cz/verejnost/api/funkcionari/[a-f0-9-]+$' and checkd is not null
order by url""")
        rows = self.cur.fetchall()
        for row in rows:
            self.convert(*row, prep)

    def convert(self, url, url_id, prep):
        action = "noting" if prep else "converting"
        print(action + " " + url + "...", file=sys.stderr)

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

        doc = json.loads(buf.decode('utf-8'))

        if prep:
            persons = self.get_entities(doc)
            for person in persons:
                c = self.multiplicity.get(person, 0)
                self.multiplicity[person] = c + 1
        else:
            self.doc_id = None
            self.url = url
            doc = self.convert_node(doc, True)
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
                wid = self.get_unique_wid(in_node)
                if wid:
                    out_node['wikidataId'] = wid
                    pair = self.get_attributes(in_node)
                    if pair:
                        out_node['birthDate'] = pair[0]
                        out_node['personUrl'] = pair[1]
        elif type(in_node) is list:
            out_node = []
            for it in in_node:
                out_node.append(self.convert_node(it, False))
        else:
            out_node = in_node

        return out_node

    def get_unique_wid(self, detail):
        persons = self.get_entities(detail)

        l = len(persons)
        if not l:
            return None

        if l > 1:
            return None

        person = persons[0]
        if self.multiplicity[person] > 1:
            return None

        return person

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            converter = Converter(cur)
            converter.run()

if __name__ == "__main__":
    main()
