all: datetime profile

datetime:
	python/date-graph.py > web/datetime.csv

profile:
	python/profile-graph.py > web/profile.json
