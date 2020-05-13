SHELL=/bin/bash
funnel_links=$(shell awk -F "[ \t]*=[ \t]*" '/^funnel_links/ {print $$2}' ampelopsis.ini)
has_statuses=$(shell echo $$(( $(funnel_links) >= 1 )) )

main: datetime datetimes volume lang sankey chord heatmap treemap distance timeline demanding

ifeq ($(has_statuses),1)
demanding: reaction bubline
else
demanding:
endif

fulltext:
	python/condensate.py
	python/text-separator.py
	python/text-stemmer.py

datetime:
	python/date-graph.py > web/datetime.csv

datetimes:
	python/datetimes-graph.py
	cp datetimes.csv datetimes.json web

sankey:
	python/network-graph.py > web/sankey.json

pinhole:
	python/pinhole-graph.py > web/sankey.json

chord:
	python/pinhole-graph.py --chord web/chord.json

tag:
	python/tag-graph.py --chord web/chord.json

redir:
	python/redir-graph.py --chord web/chord.json

indir:
	python/indir-graph.py --chord web/chord.json

lang:
	python/lang-graph.py
	cp lang.csv lang.json web

volume:
	python/volume-graph.py > web/volume.json

heatmap:
	python/dirichlet-cluster.py
	cp heatmap.csv heatmap.json web

treemap:
	python/hierarchy-graph.py > web/treemap.json

distance:
	python/cosine-distance.py > web/distance.json

jaccard:
	python/jaccard-distance.py > web/distance.json

pool:
	python/pool-distance.py > web/distance.json

reply:
	python/reply-graph.py --chord web/chord.json

timeline:
	python/timeline-graph.py > web/timeline.json

reaction:
	python/react-graph.py --segmented > web/dhist.json

repeat:
	python/repeat-graph.py > web/histogram.json

bubline:
	python/conversation-graph.py > web/bubline.json
