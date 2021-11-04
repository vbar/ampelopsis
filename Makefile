all: condensate plaintext morphodita simple bottle

condensate:
	python/condensate.py

bottle:
	python/bottle.py

simple:
	python/text-simple.py

morphodita:
	python/morphodita-stemmer.py

plaintext:
	python/text-model.py

varlen:
	python/varlen-graph.py > web/varlen.json

daytime:
	python/daytime-graph.py > web/daytime.json
