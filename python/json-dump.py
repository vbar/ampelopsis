#!/usr/bin/python3

import json
import sys
from urllib import parse
from common import make_connection
from json_lookup import JsonLookup

def print_query(qurl):
    uo = parse.urlparse(qurl)
    params = parse.parse_qsl(uo.query)
    for p in params:
        if p[0] == 'query':
            print(p[1])

    print("")

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            lookup = JsonLookup(cur)
            for a in sys.argv[1:]:
                detail = lookup.get_document(a)
                position_set = lookup.make_position_set(detail)
                qurl = lookup.make_query_url(detail, position_set)
                print_query(qurl)
                leaf = lookup.get_document(qurl)
                if leaf:
                    json.dump(leaf, sys.stdout, ensure_ascii=False)
                    print("")

                if len(position_set):
                    qurl = lookup.make_query_url(detail, set())
                    leaf = lookup.get_document(qurl)
                    if leaf:
                        print("")
                        print_query(qurl)
                        json.dump(leaf, sys.stdout, ensure_ascii=False)
                        print("")

if __name__ == "__main__":
    main()
