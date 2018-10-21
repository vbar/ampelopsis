#!/bin/bash

# not actually looking for cats - just need a simple query URL to seed...
./seed.py 'https://cro.justice.cz/verejnost/api/funkcionari?order=DESC&page=0&pageSize=100&sort=created' 'https://query.wikidata.org/sparql?format=json&query=SELECT+%3Fit+WHERE+%7B+%3Fit+wdt%3AP31+wd%3AQ146.%7D'
