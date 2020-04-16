#!/bin/sh

town_top=""
funnel_links=$(awk -F "[ \t]*=[ \t]*" '/^funnel_links/ {print $2}' ampelopsis.ini)
if [[ "$funnel_links" -eq 1 ]]; then
   town_top=https://twitter.com
fi

python/seed.py "`python/baker.py`" 'https://www.hlidacstatu.cz/api/v1/DatasetSearch/vyjadreni-politiku?desc=1&page=1&q=server%3ATwitter&sort=datum' $town_top
