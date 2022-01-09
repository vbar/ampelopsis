all: mold condensate wordlist bottle

mold:
	python/mold.py

condensate:
	python/condensate.py

bottle:
	python/bottle.py

wordlist: cache/wordlist.txt

volume:
	python/volume-graph.py > web/volume.json

cache/wordlist.txt:
	python/wordlist.py

worddist: cache/wordlist.txt
	echo "word,fraction" > web/worddist.csv
	python/random-filter.py cache/wordlist.txt >> web/worddist.csv
