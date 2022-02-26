all: mold condensate wordlist bottle

mold:
	python/mold.py

condensate:
	python/condensate.py

bottle:
	python/bottle.py

wordlist: cache/wordlist.txt

cache/wordlist.txt:
	python/wordlist.py

worddist: cache/wordlist.txt
	echo "word,fraction" > web/worddist.csv
	python/random-filter.py cache/wordlist.txt >> web/worddist.csv

lemmascan:
	python/lemmascan.py > web/lemmascan.csv
