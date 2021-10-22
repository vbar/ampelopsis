all: circuit bottle

legacy: activity category length type typelength

bottle:
	python/bottle.py

circuit:
	python/text-model.py

activity:
	python/activity-graph.py > web/activity.json

category:
	python/category-graph.py > web/category.json

length:
	python/length-graph.py > web/length.json

type:
	python/type-graph.py > web/type.json

typelength:
	python/typelength-graph.py > web/typelength.json
