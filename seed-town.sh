#!/bin/sh

alt_town_top=""
funnel_links=$(awk -F "[ \t]*=[ \t]*" '/^funnel_links/ {print $2}' ampelopsis.ini)

if [[ "$funnel_links" -eq 3 ]]; then
   alt_town_top=https://mobile.twitter.com
fi

python/seed.py https://twitter.com/strakovka $alt_town_top
