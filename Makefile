all: plaintext morphodita simple bottle

explore: category length type typelength simplelength

bottle:
	python/bottle.py

simple:
	python/text-simple.py

morphodita:
	python/morphodita-stemmer.py

plaintext:
	python/text-model.py

category:
	python/category-graph.py > web/category.json

length:
	python/length-graph.py > web/length.json

type:
	python/type-graph.py > web/type.json

typelength:
	python/typelength-graph.py > web/typelength.json

simplelength:
	python/simple-graph.py > web/simplelength.json
