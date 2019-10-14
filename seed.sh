#!/bin/sh

python/seed.py 'https://cro.justice.cz/verejnost/api/funkcionari?order=DESC&page=0&pageSize=100&sort=created' "`python/jumper.py`"

