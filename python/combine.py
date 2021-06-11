#!/usr/bin/python3

import json
import os
import shutil
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

        self.combo_dir = os.path.join(get_parent_directory(), "combo")
        if not os.path.exists(self.combo_dir):
            os.makedirs(self.combo_dir)

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

        m = self.leaf_merger.person_url_rx.match(url)
        if not m:
            raise Exception("URL %s doesn't match person" % url)

        person_id = m.group('id')
        doc = json.loads(buf.decode('utf-8'))
        self.leaf_merger.merge(person_id, doc)

        if prep:
            persons = self.get_persons(doc)
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

            if doc.get('wikidataId'):
                shutil.copy(target, self.combo_dir)

    def convert_node(self, in_node, top_level):
        if type(in_node) is dict:
            out_node = {}
            for k, v in in_node.items():
                if k == 'id':
                    k = 'Id'
                    if top_level:
                        self.doc_id = v
                elif top_level and (k == 'firstName'):
                    k = 'Name'
                elif top_level and (k == 'lastName'):
                    k = 'Surname'
                elif k == 'identificationNumber':
                    # don't transform foreign ID
                    addr = in_node.get('address')
                    if addr:
                        country = addr.get('country')
                        if country:
                            name = country.get('name')
                            if name and (name.lower() == "česká republika"):
                                k = 'ICO'

                if k in ( 'concatenatedWorkingPositionOrganizations', 'concatenatedWorkingPositions'):
                    out_node[k] = self.convert_concatenated(v)
                elif k == 'hasSecretStatement':
                    pass
                else:
                    out_node[k] = self.convert_node(v, False)
            if top_level:
                out_node['Url'] = self.url

                if self.has_secret(in_node):
                    out_node['hasSecret'] = True

                wid = self.get_unique_wid(in_node)
                if wid:
                    out_node['wikidataId'] = wid
                    pellet = self.get_attributes(in_node)
                    assert pellet

                    out_node['Birthdate'] = pellet.birthDate
                    out_node['HsProcessType'] = 'person'
                    if pellet.aboutLink:
                        out_node['personUrl'] = pellet.aboutLink
        elif type(in_node) is list:
            out_node = []
            for it in in_node:
                if (type(it) is not dict) or len(it.keys()):
                    out_node.append(self.convert_node(it, False))
        else:
            out_node = in_node

        return out_node

    def has_secret(self, detail):
        lst = detail['workingPositions']
        for it in lst:
            wp = it['workingPosition']
            vis = wp.get('visibility')
            if vis == 'SECRET':
                return True

        statements = detail.get('statements')
        if statements:
            for stm in statements:
                off = stm.get('official')
                if off:
                    vis = off.get('visibility')
                    if vis == 'SECRET':
                        return True

        return False

    def convert_concatenated(self, v):
        if v and type(v) is str:
            seen = set()
            src_lst = v.split(",")
            dst_lst = []
            for it in src_lst:
                name = it.strip()
                if name:
                    canon = name.lower()
                    if canon not in seen:
                        seen.add(canon)
                        dst_lst.append(name)

            return ", ".join(dst_lst)
        else:
            if v:
                print("concatenated value has type " + str(type(v)), file=sys.stderr)

            return self.convert_node(v, False)

    def get_unique_wid(self, detail):
        persons = self.get_persons(detail)

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
