all: circuit main

main: length type typelength

circuit:
	python/text-model.py

length:
	python/length-graph.py > web/length.json

type:
	python/type-graph.py > web/type.json

typelength:
	python/typelength-graph.py > web/typelength.json
