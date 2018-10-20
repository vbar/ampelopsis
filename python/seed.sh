#!/bin/bash

./seed.py 'https://cro.justice.cz/verejnost/api/funkcionari?order=DESC&page=0&pageSize=100&sort=created' 'https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&language=cs&search=Miroslav_Kalousek'
