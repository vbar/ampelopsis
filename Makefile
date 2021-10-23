all: plaintext bottle

legacy: category length type typelength

bottle:
	python/bottle.py

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
