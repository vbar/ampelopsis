#!/usr/bin/python3

import json
import sys
from common import make_connection
from json_lookup import JsonLookup
from urlize import print_query

def main():
    with make_connection() as conn:
        with conn.cursor() as cur:
            lookup = JsonLookup(cur)
            for a in sys.argv[1:]:
                detail = lookup.get_document(a)
                position_set = lookup.make_position_set(detail)
                qurls = lookup.make_query_urls(detail, position_set)
                for qurl in qurls:
                    print_query(qurl)

                    leaf = lookup.get_document(qurl)
                    if leaf:
                        json.dump(leaf, sys.stdout, ensure_ascii=False)
                        print("")

                if len(position_set):
                    qurl = lookup.make_query_single_url(detail, set())
                    leaf = lookup.get_document(qurl)
                    if leaf:
                        print("")
                        print_query(qurl)
                        json.dump(leaf, sys.stdout, ensure_ascii=False)
                        print("")

if __name__ == "__main__":
    main()
