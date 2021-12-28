all: mold condensate bottle

mold:
	python/mold.py

condensate:
	python/condensate.py

bottle:
	python/bottle.py

cache/wordlist.txt:
	python/wordlist.py

worddist: cache/wordlist.txt
	echo "word,fraction" > web/worddist.csv
	python/random-filter.py cache/wordlist.txt >> web/worddist.csv
