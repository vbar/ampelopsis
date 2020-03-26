#!/bin/sh

python/seed.py "`python/baker.py`" 'https://www.hlidacstatu.cz/api/v1/DatasetSearch/vyjadreni-politiku?desc=1&page=1&q=server%3ATwitter&sort=datum'
